import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="fdb_down")
node = cluster.add_instance(
    'node',
    main_configs=["configs/foundationdb.xml", "configs/named_collections.xml"],
    with_foundationdb=True,
    stay_alive=True
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)
        time.sleep(30) # Wait first boot done
        yield cluster

    finally:
        cluster.shutdown()

def exist_database(db_name):
    return node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"

def assert_timeout():
    return pytest.raises(Exception, match="Operation aborted because the transaction timed out")

def test_create():
    db_name = "test_create"
    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"CREATE DATABASE {db_name}")
    assert not exist_database(db_name)

    cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")
    assert exist_database(db_name)
    
def test_rename():
    db_name = "test_rename"

    cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")

    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"RENAME DATABASE {db_name} TO {db_name}2")
    assert exist_database(db_name)
    assert not exist_database(db_name + '2')

    cluster.start_fdb()
    node.query(f"RENAME DATABASE {db_name} TO {db_name}2")
    assert not exist_database(db_name)
    assert exist_database(db_name + '2')

def test_drop():
    db_name = "test_drop"

    cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")

    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DROP DATABASE {db_name}")

    # When fdb failed, the database will be detached.
    assert not exist_database(db_name)

    cluster.start_fdb()
    node.query(f"ATTACH DATABASE {db_name}")
    node.query(f"DROP DATABASE {db_name}")
    assert not exist_database(db_name)