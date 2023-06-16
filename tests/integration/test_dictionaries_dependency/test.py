import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", stay_alive=True)
node2 = cluster.add_instance(
    "node2", stay_alive=True, main_configs=["configs/disable_lazy_load.xml"]
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
