import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", stay_alive=True, main_configs=["configs/overrides.xml"]
)
node2 = cluster.add_instance(
    "node2",
    stay_alive=True,
    main_configs=["configs/disable_lazy_load.xml", "configs/overrides.xml"],
)
nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        for node in nodes:
            node.query("CREATE DATABASE IF NOT EXISTS test")
            # Different internal dictionary name with Atomic
            node.query(
                "CREATE DATABASE IF NOT EXISTS test_ordinary ENGINE=Ordinary",
                settings={"allow_deprecated_database_ordinary": 1},
            )
            node.query("CREATE DATABASE IF NOT EXISTS atest")
            node.query("CREATE DATABASE IF NOT EXISTS ztest")
            node.query("CREATE TABLE test.source(x UInt64, y UInt64) ENGINE=Log")
            node.query("INSERT INTO test.source VALUES (5,6)")

            for db in ("test", "test_ordinary"):
                node.query(
                    "CREATE DICTIONARY {}.dict(x UInt64, y UInt64) PRIMARY KEY x "
                    "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'source' DB 'test')) "
                    "LAYOUT(FLAT()) LIFETIME(0)".format(db)
                )
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        for node in nodes:
            for i in range(4):
                node.query("DROP DICTIONARY IF EXISTS test.other_{}".format(i))
            node.query("DROP DICTIONARY IF EXISTS test.adict")
            node.query("DROP DICTIONARY IF EXISTS test.zdict")
            node.query("DROP DICTIONARY IF EXISTS atest.dict")
            node.query("DROP DICTIONARY IF EXISTS ztest.dict")
            node.query("DROP TABLE IF EXISTS test.atbl")
            node.query("DROP TABLE IF EXISTS test.ztbl")
            node.query("DROP TABLE IF EXISTS atest.tbl")
            node.query("DROP TABLE IF EXISTS ztest.tbl")
            node.query("DROP DATABASE IF EXISTS dict_db")


@pytest.mark.parametrize("node", nodes)
def test_dependency_via_implicit_table(node):
    d_names = ["test.adict", "test.zdict", "atest.dict", "ztest.dict"]
    for d_name in d_names:
        node.query(
            "CREATE DICTIONARY {}(x UInt64, y UInt64) PRIMARY KEY x "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict' DB 'test')) "
            "LAYOUT(FLAT()) LIFETIME(0)".format(d_name)
        )

    def check():
        for d_name in d_names:
            assert (
                node.query("SELECT dictGet({}, 'y', toUInt64(5))".format(d_name))
                == "6\n"
            )

    check()

    # Restart must not break anything.
    node.restart_clickhouse()
    check()


@pytest.mark.parametrize("node", nodes)
def test_dependency_via_explicit_table(node):
    tbl_names = ["test.atbl", "test.ztbl", "atest.tbl", "ztest.tbl"]
    d_names = ["test.other_{}".format(i) for i in range(0, len(tbl_names))]
    for i in range(0, len(tbl_names)):
        tbl_name = tbl_names[i]
        tbl_database, tbl_shortname = tbl_name.split(".")
        d_name = d_names[i]
        node.query(
            "CREATE TABLE {}(x UInt64, y UInt64) ENGINE=Dictionary('test.dict')".format(
                tbl_name
            )
        )
        node.query(
            "CREATE DICTIONARY {}(x UInt64, y UInt64) PRIMARY KEY x "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE '{}' DB '{}')) "
            "LAYOUT(FLAT()) LIFETIME(0)".format(d_name, tbl_shortname, tbl_database)
        )

    def check():
        for d_name in d_names:
            assert (
                node.query("SELECT dictGet({}, 'y', toUInt64(5))".format(d_name))
                == "6\n"
            )

    check()

    # Restart must not break anything.
    node.restart_clickhouse()
    check()
    for dct in d_names:
        node.query(f"DROP DICTIONARY {dct}")
    for tbl in tbl_names:
        node.query(f"DROP TABLE {tbl}")


@pytest.mark.parametrize("node", nodes)
def test_dependency_via_dictionary_database(node):
    node.query("CREATE DATABASE dict_db ENGINE=Dictionary")

    d_names = ["test_ordinary.adict", "test_ordinary.zdict", "atest.dict", "ztest.dict"]
    for d_name in d_names:
        node.query(
            "CREATE DICTIONARY {}(x UInt64, y UInt64) PRIMARY KEY x "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'test_ordinary.dict' DB 'dict_db')) "
            "LAYOUT(FLAT()) LIFETIME(0)".format(d_name)
        )

    def check():
        for d_name in d_names:
            assert (
                node.query("SELECT dictGet({}, 'y', toUInt64(5))".format(d_name))
                == "6\n"
            )

    for d_name in d_names:
        assert (
            node.query("SELECT dictGet({}, 'y', toUInt64(5))".format(d_name)) == "6\n"
        )

    # Restart must not break anything.
    node.restart_clickhouse()
    for d_name in d_names:
        assert (
            node.query_with_retry("SELECT dictGet({}, 'y', toUInt64(5))".format(d_name))
            == "6\n"
        )

    # cleanup
    for d_name in d_names:
        node.query(f"DROP DICTIONARY IF EXISTS {d_name} SYNC")
    node.query("DROP DATABASE dict_db SYNC")
    node.restart_clickhouse()


@pytest.mark.parametrize("node", nodes)
def test_dependent_dict_table_distr(node):
    query = node.query
    query("CREATE DATABASE test_db;")
    query(
        "CREATE TABLE test_db.test(id UInt32,data UInt32,key1 UInt8,key2 UInt8) ENGINE=MergeTree  ORDER BY id;"
    )
    query(
        "INSERT INTO test_db.test SELECT  abs(rand32())%100, rand32()%1000, abs(rand32())%1, abs(rand32())%1  FROM numbers(100);"
    )
    query(
        "CREATE TABLE test_db.dictback (key1 UInt8,key2 UInt8, value UInt8) ENGINE=MergeTree  ORDER BY key1;"
    )
    query("INSERT INTO test_db.dictback VALUES (0,0,0);")

    query(
        "CREATE DICTIONARY test_db.mdict (key1 UInt8,key2 UInt8, value UInt8) PRIMARY KEY key1,key2"
        " SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'test_db' TABLE 'dictback'))"
        " LIFETIME(MIN 100 MAX 100)  LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1000));"
    )

    query(
        "CREATE TABLE test_db.distr (id UInt32, data UInt32, key1 UInt8, key2 UInt8)"
        " ENGINE = Distributed('test_shard_localhost', test_db, test, dictGetOrDefault('test_db.mdict','value',(key1,key2),0));"
    )

    # Tables should load in the correct order.
    node.restart_clickhouse()

    query("DETACH TABLE test_db.distr;")
    query("ATTACH TABLE test_db.distr;")

    node.restart_clickhouse()

    query("DROP DATABASE IF EXISTS test_db;")


def test_no_lazy_load():
    node2.query("create database no_lazy")
    node2.query(
        "create table no_lazy.src (n int, m int) engine=MergeTree order by n partition by n % 100"
    )
    node2.query("insert into no_lazy.src select number, number from numbers(0, 99)")
    node2.query("insert into no_lazy.src select number, number from numbers(100, 99)")
    node2.query(
        "create dictionary no_lazy.dict (n int, mm int) primary key n "
        "source(clickhouse(query 'select n, m + sleepEachRow(0.1) as mm from no_lazy.src')) "
        "lifetime(min 0 max 0) layout(complex_key_hashed_array(shards 10))"
    )

    node2.restart_clickhouse()

    assert "42\n" == node2.query("select dictGet('no_lazy.dict', 'mm', 42)")

    assert "some tables depend on it" in node2.query_and_get_error(
        "drop table no_lazy.src", settings={"check_referential_table_dependencies": 1}
    )

    node2.query("drop database no_lazy")
