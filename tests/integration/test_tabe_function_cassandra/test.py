import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from cassandra.cluster import Cluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_cassandra=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def create_table_cassandra(session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute("USE test;")
    session.execute("CREATE TABLE IF NOT EXISTS simple_table (key int PRIMARY KEY, data text);")


def test_simple_select(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    create_table_cassandra(session)
    for i in range(0, 50):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, '{str(i * i)}');")

    node = started_cluster.instances["node"]

    assert node.query("SELECT COUNT() FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering')") == "50\n"
    assert (
        node.query("SELECT sum(key) FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering')") 
        == str(sum(range(0, 50))) + "\n"
    )
    assert (
        node.query("SELECT data from cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering') where key = 20")
        == str(20 * 20) + "\n"
    )
    session.execute("DROP TABLE test.simple_table;")


def test_allow_filtering(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    create_table_cassandra(session)
    for i in range(0, 10):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, '{str(i * i)}');")
    
    for i in range(10, 20):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, 'test');")
  
    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '') WHERE data = 'test'")

    assert (
        node.query("SELECT COUNT() FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering') WHERE data = 'test' ") 
        == "10\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT key FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '') WHERE data = 'test'")
    
    assert (
        node.query("SELECT sum(key) FROM cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering') WHERE data = 'test' ") 
        == str(sum(range(10, 20))) + "\n"
    )

    session.execute("DROP TABLE test.simple_table;")



def test_filtering_with_index(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute("USE test;")
    session.execute("CREATE TABLE simple_table_new (key int PRIMARY KEY, data int, name text);")
    for i in range(0, 10):
        session.execute(f"INSERT INTO simple_table_new (key, data, name) VALUES ({i}, {i * i}, '{'a' * (i + 1)}');")

    session.execute("CREATE INDEX name ON test.simple_table_new (name);")

    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT sum(data) FROM cassandra('cassandra1:9042', 'test', 'simple_table_new', 'One', '', '', '') WHERE data = 0")
    
    assert (
        node.query("SELECT sum(data) FROM cassandra('cassandra1:9042', 'test', 'simple_table_new', 'One', '', '', '') WHERE name = 'aa'") 
        == "1\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT Count() FROM cassandra('cassandra1:9042', 'test', 'simple_table_new', 'One', '', '', '') WHERE data = 0")
    
    assert (
        node.query("SELECT Count() FROM cassandra('cassandra1:9042', 'test', 'simple_table_new', 'One', '', '', '') WHERE name = 'aa'") 
        == "1\n"
    )