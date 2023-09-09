import pytest
from helpers.cluster import ClickHouseCluster
from textwrap import dedent


db_name = "test_fdb_down"

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__, name="fdb_down")
        node = cluster.add_instance(
            'node',
            main_configs=["configs/foundationdb.xml"],
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)
        node = cluster.instances["node"]
        node.query(f"CREATE DATABASE {db_name}")
        assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
        yield cluster

    finally:
        cluster.shutdown()
    
def exist_table(table_name, node):
    return node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name ='{table_name}'").strip() == "1"

def assert_timeout():
    return pytest.raises(Exception, match="Operation aborted because the transaction timed out")

def test_create(started_cluster):
    table_name = "test_create"
    node = started_cluster.instances["node"]
    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    assert not exist_table(table_name, node)

    started_cluster.start_fdb()
    # node.query(f"ATTACH DATABASE {db_name}")
    assert not exist_table(table_name, node)
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    assert exist_table(table_name, node)
    
def test_rename(started_cluster):
    table_name = "test_rename"
    node = started_cluster.instances["node"]
    started_cluster.start_fdb()
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"RENAME Table {db_name}.{table_name} TO {db_name}.{table_name}2")
    assert exist_table(table_name, node)
    assert not exist_table(table_name + '2', node)

    started_cluster.start_fdb()
    node.query(f"RENAME Table {db_name}.{table_name} TO {db_name}.{table_name}2")
    assert not exist_table(table_name, node)
    assert exist_table(table_name + '2', node)

def test_drop(started_cluster):
    table_name = "test_drop"
    node = started_cluster.instances["node"]
    started_cluster.start_fdb()
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DROP TABLE {db_name}.{table_name}")

    assert exist_table(table_name, node)

    started_cluster.start_fdb()
    node.query(f"DROP TABLE {db_name}.{table_name}")
    assert not exist_table(table_name, node)

def test_detach_permanently(started_cluster):
    table_name = "test_detach_permanently"
    started_cluster.start_fdb()
    node = started_cluster.instances["node"]
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    started_cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DETACH TABLE {db_name}.{table_name} PERMANENTLY")
    #Table will be detached when fdb stop, so need reattach table after fdb restart 
    assert not exist_table(table_name, node)

    started_cluster.start_fdb()
    node.query(f"ATTACH TABLE {db_name}.{table_name}")
    node.query(f"DETACH TABLE {db_name}.{table_name} PERMANENTLY")
    assert not exist_table(table_name, node)

def test_alter(started_cluster):
    table_name = "test_alter"
    node = started_cluster.instances["node"]
    started_cluster.start_fdb()
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{table_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )

    started_cluster.stop_fdb()
    
    node.query(f"ALTER TABLE {db_name}.{table_name} ADD COLUMN Added1 UInt32 FIRST")
    # with assert_timeout():

    started_cluster.start_fdb()
    assert node.query(f"SHOW CREATE TABLE {db_name}.{table_name}") == f"CREATE TABLE test_fdb_down.test_alter\\n(\\n    `n` UInt64,\\n    `m` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n\\nORDER BY n\\nSETTINGS index_granularity = 8192\n"
