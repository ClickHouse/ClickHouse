import pytest
from helpers.cluster import ClickHouseCluster

from textwrap import dedent
cluster = ClickHouseCluster(__file__, name="fdb_down")
node = cluster.add_instance(
    'node',
    main_configs=["configs/foundationdb.xml"],
    with_foundationdb=True,
    stay_alive=True
)

db_name = "test_fdb_down"
@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)
        node.query(f"CREATE DATABASE {db_name}")
        assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"

        yield cluster

    finally:
        cluster.shutdown()

def exist_table(table_name):
    return node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name ='{table_name}'").strip() == "1"

def assert_timeout():
    return pytest.raises(Exception, match="Operation aborted because the transaction timed out")

def test_create():
    table_name = "test_create"
    cluster.stop_fdb()
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
    assert not exist_table(table_name)

    cluster.start_fdb()
    # node.query(f"ATTACH DATABASE {db_name}")
    assert not exist_table(table_name)
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
    assert exist_table(table_name)
    
def test_rename():
    table_name = "test_rename"

    cluster.start_fdb()
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
    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"RENAME Table {db_name}.{table_name} TO {db_name}.{table_name}2")
    assert exist_table(table_name)
    assert not exist_table(table_name + '2')

    cluster.start_fdb()
    node.query(f"RENAME Table {db_name}.{table_name} TO {db_name}.{table_name}2")
    assert not exist_table(table_name)
    assert exist_table(table_name + '2')

def test_drop():
    table_name = "test_drop"

    cluster.start_fdb()
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
    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DROP TABLE {db_name}.{table_name}")

    assert exist_table(table_name)

    cluster.start_fdb()
    node.query(f"DROP TABLE {db_name}.{table_name}")
    assert not exist_table(table_name)

def test_detach_permanently():
    table_name = "test_detach_permanently"
    cluster.start_fdb()

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
    cluster.stop_fdb()
    with assert_timeout():
        node.query(f"DETACH TABLE {db_name}.{table_name} PERMANENTLY")
    #Table will be detached when fdb stop, so need reattach table after fdb restart 
    assert not exist_table(table_name)

    cluster.start_fdb()
    node.query(f"ATTACH TABLE {db_name}.{table_name}")
    node.query(f"DETACH TABLE {db_name}.{table_name} PERMANENTLY")
    assert not exist_table(table_name)

def test_alter():
    table_name = "test_alter"

    cluster.start_fdb()
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

    cluster.stop_fdb()
    
    node.query(f"ALTER TABLE {db_name}.{table_name} ADD COLUMN Added1 UInt32 FIRST")
    # with assert_timeout():

    cluster.start_fdb()
    assert node.query(f"SHOW CREATE TABLE {db_name}.{table_name}") == f"CREATE TABLE test_fdb_down.test_alter\\n(\\n    `n` UInt64,\\n    `m` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n\\nORDER BY n\\nSETTINGS index_granularity = 8192\n"
