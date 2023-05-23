import pytest
import time
from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__, name = "fdb_down")
        node = cluster.add_instance(
            'node',
            main_configs=["configs/foundationdb.xml"],
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)
        time.sleep(30)
        yield cluster

    finally:
        cluster.shutdown()

def exist_database(db_name, node):
    return node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"

def assert_timeout():
    return pytest.raises(Exception, match="Operation aborted because the transaction timed out")

def test_create(started_cluster):
    db_name = "test_create"
    node = started_cluster.instances["node"]
    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"CREATE DATABASE {db_name}")
    assert not exist_database(db_name, node)

    started_cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")
    assert exist_database(db_name, node)
    
def test_rename(started_cluster):
    db_name = "test_rename"
    node = started_cluster.instances["node"]
    started_cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")

    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"RENAME DATABASE {db_name} TO {db_name}2")
    assert exist_database(db_name, node)
    assert not exist_database(db_name + '2', node)

    started_cluster.start_fdb()
    node.query(f"RENAME DATABASE {db_name} TO {db_name}2")
    assert not exist_database(db_name, node)
    assert exist_database(db_name + '2', node)

def test_drop(started_cluster):
    db_name = "test_drop"
    node = started_cluster.instances["node"]
    started_cluster.start_fdb()
    node.query(f"CREATE DATABASE {db_name}")

    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DROP DATABASE {db_name}")

    # When fdb failed, the database will be detached.
    assert not exist_database(db_name, node)

    started_cluster.start_fdb()
    node.query(f"ATTACH DATABASE {db_name}")
    node.query(f"DROP DATABASE {db_name}")
    assert not exist_database(db_name, node)