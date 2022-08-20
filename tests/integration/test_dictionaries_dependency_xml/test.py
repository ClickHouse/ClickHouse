import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

DICTIONARY_FILES = [
    "configs/dictionaries/dep_x.xml",
    "configs/dictionaries/dep_y.xml",
    "configs/dictionaries/dep_z.xml",
    "configs/dictionaries/node.xml",
]

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", dictionaries=DICTIONARY_FILES, stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance.query(
            """
            CREATE DATABASE IF NOT EXISTS dict ENGINE=Dictionary;
            CREATE DATABASE IF NOT EXISTS test;
            DROP TABLE IF EXISTS test.elements;
            CREATE TABLE test.elements (id UInt64, a String, b Int32, c Float64) ENGINE=Log;
            INSERT INTO test.elements VALUES (0, 'water', 10, 1), (1, 'air', 40, 0.01), (2, 'earth', 100, 1.7);
            """
        )

        yield cluster

    finally:
        cluster.shutdown()


def get_status(dictionary_name):
    return instance.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


def test_get_data(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so these dictionary are not loaded.
    assert get_status("dep_x") == "NOT_LOADED"
    assert get_status("dep_y") == "NOT_LOADED"
    assert get_status("dep_z") == "NOT_LOADED"

    # Dictionary 'dep_x' depends on 'dep_z', which depends on 'dep_y'.
    # So they all should be loaded at once.
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(1))") == "air\n"
    assert get_status("dep_x") == "LOADED"
    assert get_status("dep_y") == "LOADED"
    assert get_status("dep_z") == "LOADED"

    # Other dictionaries should work too.
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(1))") == "air\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(1))") == "air\n"

    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "YY\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # Update the source table.
    query("INSERT INTO test.elements VALUES (3, 'fire', 30, 8)")

    # Wait for dictionaries to be reloaded.
    assert_eq_with_retry(
        instance,
        "SELECT dictHas('dep_x', toUInt64(3))",
        "1",
        sleep_time=2,
        retry_count=10,
    )
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "fire\n"

    # dep_z (and hence dep_x) are updated only when there `intDiv(count(), 4)` is changed, now `count()==4`,
    # so dep_x and dep_z are not going to be updated after the following INSERT.
    query("INSERT INTO test.elements VALUES (4, 'ether', 404, 0.001)")
    assert_eq_with_retry(
        instance,
        "SELECT dictHas('dep_y', toUInt64(4))",
        "1",
        sleep_time=2,
        retry_count=10,
    )
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(4))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(4))") == "ZZ\n"


def dependent_tables_assert():
    res = instance.query("select database || '.' || name from system.tables")
    assert "system.join" in res
    assert "default.src" in res
    assert "dict.dep_y" in res
    assert "lazy.log" in res
    assert "test.d" in res
    assert "default.join" in res
    assert "a.t" in res


def test_dependent_tables(started_cluster):
    query = instance.query
    query("create database lazy engine=Lazy(10)")
    query("create database a")
    query("create table lazy.src (n int, m int) engine=Log")
    query(
        "create dictionary a.d (n int default 0, m int default 42) primary key n "
        "source(clickhouse(host 'localhost' port tcpPort() user 'default' table 'src' password '' db 'lazy'))"
        "lifetime(min 1 max 10) layout(flat())"
    )
    query("create table system.join (n int, m int) engine=Join(any, left, n)")
    query("insert into system.join values (1, 1)")
    query(
        "create table src (n int, m default joinGet('system.join', 'm', 1::int),"
        "t default dictGetOrNull('a.d', 'm', toUInt64(3)),"
        "k default dictGet('a.d', 'm', toUInt64(4))) engine=MergeTree order by n"
    )
    query(
        "create dictionary test.d (n int default 0, m int default 42) primary key n "
        "source(clickhouse(host 'localhost' port tcpPort() user 'default' table 'src' password '' db 'default'))"
        "lifetime(min 1 max 10) layout(flat())"
    )
    query(
        "create table join (n int, m default dictGet('a.d', 'm', toUInt64(3)),"
        "k default dictGet('test.d', 'm', toUInt64(0))) engine=Join(any, left, n)"
    )
    query(
        "create table lazy.log (n default dictGet(test.d, 'm', toUInt64(0))) engine=Log"
    )
    query(
        "create table a.t (n default joinGet('system.join', 'm', 1::int),"
        "m default dictGet('test.d', 'm', toUInt64(3)),"
        "k default joinGet(join, 'm', 1::int)) engine=MergeTree order by n"
    )

    dependent_tables_assert()
    instance.restart_clickhouse()
    dependent_tables_assert()
    query("drop table a.t")
    query("drop table lazy.log")
    query("drop table join")
    query("drop dictionary test.d")
    query("drop table src")
    query("drop table system.join")
    query("drop database a")
    query("drop database lazy")


def test_xml_dict_same_name(started_cluster):
    instance.query(
        "create table default.node ( key UInt64, name String ) Engine=Dictionary(node);"
    )
    instance.restart_clickhouse()
    assert "node" in instance.query("show tables from default")
    instance.query("drop table default.node")
