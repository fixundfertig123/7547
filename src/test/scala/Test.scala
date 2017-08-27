import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.HashMap

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import org.scalatest.Suites

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientVertex

class Test extends Suites(Test2) {}

object Test2 extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  ODatabaseRecordThreadLocal.INSTANCE

  val graphFactory: OrientGraphFactory = new OrientGraphFactory("memory:test").setupPool(1, 5)
  //  val graphFactory: OrientGraphFactory = new OrientGraphFactory("remote:localhost/test", "test", "test1234")  

  var graph: OrientBaseGraph = null

  override def beforeEach() {
    graph = graphFactory.getTx
    graph.getRawGraph.reload
    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
    super.afterEach() // To be stackable, must call super.afterEach
    if (!graph.isClosed()) {
      graph.shutdown()
    }
  }

  override def afterAll() {
    super.afterAll() // To be stackable, must call super.afterEach
    if (!graph.isClosed()) {
      graphFactory.close()
    }
  }

  println("Start!")

  graphFactory.getNoTx.command(new OCommandSQL("DROP CLASS CV1")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("DROP CLASS CV2")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("DROP CLASS SV")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("DROP SEQUENCE seqCounter")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("DROP index testID")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("DROP index uniqueID")).execute();

  graphFactory.getNoTx.command(new OCommandSQL("CREATE CLASS SV extends V")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE SEQUENCE seqCounter TYPE ORDERED")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE PROPERTY SV.uniqueID Long")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE PROPERTY SV.testID Long")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID NOTNULL true")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID MANDATORY true")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID READONLY true")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID DEFAULT 'sequence(\"seqCounter\").next()'")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE CLASS CV1 extends SV")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE CLASS CV2 extends SV")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE INDEX uniqueID ON SV (uniqueID) UNIQUE")).execute();
  graphFactory.getNoTx.command(new OCommandSQL("CREATE INDEX testid ON SV (testID) UNIQUE")).execute();

  var v1: OrientVertex = null
  var v2: OrientVertex = null

  "A vertex creation" should "be possible" in {
    try {
      v1 = graph.addVertex(OrientBaseGraph.CLASS_PREFIX + "CV1".asInstanceOf[Object], HashMap("testID" -> 1, 1 -> "b", 2 -> "a", 3 -> "c").asJava)
      v1.detach()
    } catch {
      case e: Throwable => {
        throw e
      }
    } finally {
      graph.shutdown();
    }
  }

  it should "throw an Exception if duplicate id is entered, causing an index error on commit" in {
    try {
      intercept[ORecordDuplicatedException] {
        graph.addVertex(OrientBaseGraph.CLASS_PREFIX + "CV1".asInstanceOf[Object], HashMap("testID" -> 1, 1 -> "b", 2 -> "a", 3 -> "c").asJava)
        graph.commit()
      }
    } finally {
      graph.shutdown
    }
  }

  "A different vertex creation" should "be possible" in {
    try {
      v1.attach(graph).reload()
      val v2 = graph.addVertex(OrientBaseGraph.CLASS_PREFIX + "CV2".asInstanceOf[Object], HashMap("testID" -> 2, 1 -> "b", 2 -> "a", 3 -> "c").asJava)
      println(v1.getProperties.toString() + " / " + v2.getProperties.toString())
    } catch {
      case e: Throwable => {
        throw e
      }
    } finally {
      graph.shutdown();
      println("Finished!")
    }
  }

  graphFactory.close()
}