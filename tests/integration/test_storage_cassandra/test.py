
import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster
from cassandra.cluster import Cluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/named_collection.xml"], with_cassandra=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()



def test_simple_select(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute("USE test;")
    session.execute("CREATE TABLE simple_table (key int PRIMARY KEY, data text);")
    for i in range(0, 100):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, '{str(i * i)}');")


    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_cassandra_table (key Integer, data String) ENGINE = Cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering')"
    )

    assert node.query("SELECT COUNT() FROM simple_cassandra_table") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM simple_cassandra_table") 
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data from simple_cassandra_table where key = 20")
        == str(20 * 20) + "\n"
    )
    node.query("DROP TABLE simple_cassandra_table")
    session.execute("DROP TABLE test.simple_table;")


def test_incorrect_data_type(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute("USE test;")
    session.execute("CREATE TABLE simple_table (key int PRIMARY KEY, data text);")
    for i in range(0, 100):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, '{str(i * i)}');")
    
    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_cassandra_table (key String, data String) ENGINE = Cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM simple_cassandra_table")
    
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT sum(key) FROM simple_cassandra_table")
    
    node.query("DROP TABLE simple_cassandra_table")
    session.execute("DROP TABLE test.simple_table;")

#указан ли allow_filtering
def test_allow_filtering(started_cluster):
    started_cluster.cassandra_ip = started_cluster.get_instance_ip(started_cluster.cassandra_host)
    cass_client = Cluster(
        [started_cluster.cassandra_ip],
        port=started_cluster.cassandra_port,
    )
    session = cass_client.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute("USE test;")
    session.execute("CREATE TABLE simple_table (key int PRIMARY KEY, data text);")
    for i in range(0, 10):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, '{str(i * i)}');")
    
    for i in range(10, 20):
        session.execute(f"INSERT INTO simple_table (key, data) VALUES ({i}, 'test');")

    
    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_cassandra_table_right (key Integer, data String) ENGINE = Cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', 'allow_filtering')"
    )

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_cassandra_table_wrong (key Integer, data String) ENGINE = Cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM simple_cassandra_table_wrong WHERE data = 'test'")

    assert (
        node.query("SELECT COUNT() FROM simple_cassandra_table_right WHERE data = 'test' ") 
        == "10\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT key FROM simple_cassandra_table_wrong WHERE data = 'test'")
    
    assert (
        node.query("SELECT sum(key) FROM simple_cassandra_table_right WHERE data = 'test' ") 
        == str(sum(range(10, 20))) + "\n"
    )

    node.query("DROP TABLE simple_cassandra_table_right")
    node.query("DROP TABLE simple_cassandra_table_wrong")

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
    session.execute("CREATE TABLE simple_table (key int PRIMARY KEY, data int, name text);")
    for i in range(0, 10):
        session.execute(f"INSERT INTO simple_table (key, data, name) VALUES ({i}, {i * i}, '{'a' * (i + 1)}');")

    session.execute("CREATE INDEX name ON test.simple_table (name);")

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_cassandra_table (key Integer, data Integer, name String) ENGINE = Cassandra('cassandra1:9042', 'test', 'simple_table', 'One', '', '', '')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT sum(data) FROM simple_cassandra_table WHERE data = 0")
    
    assert (
        node.query("SELECT sum(data) FROM simple_cassandra_table WHERE name = 'aa'") 
        == "1\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT Count() FROM simple_cassandra_table WHERE data = 0")
    
    assert (
        node.query("SELECT Count() FROM simple_cassandra_table WHERE name = 'aa'") 
        == "1\n"
    )



    






