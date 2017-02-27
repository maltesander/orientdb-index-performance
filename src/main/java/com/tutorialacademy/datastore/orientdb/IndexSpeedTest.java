package com.tutorialacademy.datastore.orientdb;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class IndexSpeedTest 
{
	/** Create a unique case-insensitive index for a vertex string property */
	public static void createUniqueVertexStringIndexCI( OrientGraphNoTx graph, String vertexClass, String property ) {
		
		String indexName = vertexClass + "." + property + ".index";
		// check if vertex class already exists -> otherwise create
		if( graph.getVertexType( vertexClass ) == null ) {
			graph.createVertexType( vertexClass );
		}
		OrientVertexType type = graph.getVertexType( vertexClass );
		// check if index already exists -> otherwise create
		if( type.getClassIndex( indexName ) == null ) {
			type.createProperty( property, OType.STRING ).setMandatory(true).setNotNull(true).setCollate( OCaseInsensitiveCollate.NAME );
			type.createIndex( indexName, INDEX_TYPE.UNIQUE_HASH_INDEX, property );
		}
	}
	
	/** Create a unique index for a vertex string property */
	public static void createUniqueVertexStringIndex( OrientGraphNoTx graph, String vertexClass, String property ) {
		
		String indexName = vertexClass + "." + property + ".index";
		// check if vertex class already exists -> otherwise create
		if( graph.getVertexType( vertexClass ) == null ) {
			graph.createVertexType( vertexClass );
		}
		OrientVertexType type = graph.getVertexType( vertexClass );
		// check if index already exists -> otherwise create
		if( type.getClassIndex( indexName ) == null ) {
			type.createProperty( property, OType.STRING ).setMandatory(true).setNotNull(true); 
			type.createIndex( indexName, INDEX_TYPE.UNIQUE_HASH_INDEX, property );
		}
	}
	
	public static void main(String[] args) {
		// create index or not
		boolean createIndex = false;
		// number of elements to use
		int vertexCount = 1000000;
		// vertex class
		String vertexClass = "user";
		// vertex orient class
		String orientVertexClass = "class:" + vertexClass;
		// indexed property
		String property = "email";
		// "property" to find
		String vertexToFind = "email544889@tutorial-academy.com";
		// iterations to test
		int iterations = 5;
		// time measurement
		long insertDuration = 0;
		long createIndexDuration = 0;
		long commitDuration = 0;
		long queryDuration = 0;

		System.out.println("Start evaluation ...");
		
		for( int i = 1; i <= iterations; i++ ) {
			//OrientGraphFactory factory = new OrientGraphFactory( "memory:testdb", "admin", "admin" ).setupPool( 1, 5 );
			// We use testdb + i here: Otherwise some resources are not properly freed and the index is still existing in the next round
			OrientGraphFactory factory = new OrientGraphFactory(  "plocal:testdb" + i, "admin", "admin" ).setupPool( 1, 5 );
			//OrientGraphFactory factory = new OrientGraphFactory( "remote:localhost/testdb", "root", "root" ).setupPool( 1, 5 );
			// prepare the graph factory for a big insert operation
			factory.declareIntent( new OIntentMassiveInsert() );
		    
			// build index
			if( createIndex ) {
				TimeWatch indexTimer = TimeWatch.start();
				// we use non-transactional graph for indexing
				OrientGraphNoTx noTx = factory.getNoTx();
				createUniqueVertexStringIndex( noTx, vertexClass, property );
				noTx.shutdown(true);
				
				long temp = indexTimer.time( TimeUnit.MILLISECONDS );
				createIndexDuration += temp ;
				
				System.out.println("[" + i + "] Created Index in " + temp + "ms" );
			}
			
			// use tx and noTx graph for testing
			OrientGraphNoTx graph = factory.getNoTx();
			//OrientGraph graph = factory.getTx();

			TimeWatch insertTimer = TimeWatch.start();
			
			for( int j = 0; j < vertexCount; j++ ) {
				String userEmail = ("email" + j + "@tutorial-academy.com");
				// add new vertex with email (setProperty does not work because we have a mandatory index -> must be available at creation time)
				graph.addVertex( orientVertexClass, property, userEmail );
				if( j % 10000 == 0 ) {
					System.out.println( "[" + i + "] Inserting: " + Math.round( (double) j / (double) vertexCount * 100 )  + "%" );
				}
			}
			
			long temp = insertTimer.time( TimeUnit.MILLISECONDS );
			insertDuration += temp;
			
			System.out.println("[" + i + "] Insert duration: " + temp );
			
			TimeWatch commitTimer = TimeWatch.start();
			graph.commit();
			temp = commitTimer.time( TimeUnit.MILLISECONDS );
			commitDuration += temp;
			
			System.out.println("[" + i + "] Commit duration: " + temp );
			
			// read vertex
			TimeWatch queryTimer = TimeWatch.start();
			Iterable<Vertex> vertices = graph.getVertices( property, vertexToFind );
			Iterator<Vertex> iterator = vertices.iterator();
		
			if( iterator.hasNext() )
			{
				Vertex v = iterator.next();
				
				System.out.println( "[" + i + "] Found element: [" + v.getId() + "] - Property=" + v.getProperty( property ) );
			}
			
			temp = queryTimer.time( TimeUnit.MILLISECONDS );
			queryDuration += temp;
			System.out.println("[" + i + "] Query duration: " + temp + "ms");
			
			graph.shutdown( true );
	
			factory.close();
		}
		
		System.out.println("\nResults:");
		System.out.println("Insert duration: " + insertDuration / iterations  + "ms");
		System.out.println("CreateIndex duration: " + createIndexDuration / iterations  + "ms");
		System.out.println("Commit duration: " + commitDuration / iterations  + "ms");
		System.out.println("Query duration: " + queryDuration / iterations  + "ms");
	}
}
