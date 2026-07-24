import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)
ch3 = cluster.add_instance(
    "ch3",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)
ch4 = cluster.add_instance(
    "ch4",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch1.query(
            "CREATE TABLE sometbl ON CLUSTER 'cluster' (key UInt64, value String) ENGINE = MergeTree ORDER by key"
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_dictionary_ddl_on_cluster(started_cluster):
    for node in [ch1, ch2, ch3, ch4]:
        assert node.query("SELECT count() from sometbl") == "0\n"

    for num, node in enumerate([ch1, ch2, ch3, ch4]):
        node.query("insert into sometbl values ({}, '{}')".format(num, node.name))

    ch1.query(
        """
        CREATE DICTIONARY somedict ON CLUSTER 'cluster' (
            key UInt64,
            value String
        )
        PRIMARY KEY key
        LAYOUT(FLAT())
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'sometbl' DB 'default'))
        LIFETIME(10)
        """
    )

    for num, node in enumerate([ch1, ch2, ch3, ch4]):
        assert node.query("SELECT count() from sometbl") == "1\n"
        assert (
            node.query(
                "SELECT dictGetString('default.somedict', 'value', toUInt64({}))".format(
                    num
                )
            )
            == node.name + "\n"
        )

    ch1.query("DETACH DICTIONARY default.somedict ON CLUSTER 'cluster'")

    for node in [ch1, ch2, ch3, ch4]:
        with pytest.raises(QueryRuntimeException):
            node.query("SELECT dictGetString('default.somedict', 'value', toUInt64(1))")

    ch1.query("ATTACH DICTIONARY default.somedict ON CLUSTER 'cluster'")

    for num, node in enumerate([ch1, ch2, ch3, ch4]):
        assert node.query("SELECT count() from sometbl") == "1\n"
        assert (
            node.query(
                "SELECT dictGetString('default.somedict', 'value', toUInt64({}))".format(
                    num
                )
            )
            == node.name + "\n"
        )

    for num, node in enumerate([ch1, ch2, ch3, ch4]):
        node.query("ALTER TABLE sometbl UPDATE value = 'new_key' WHERE 1")

    ch1.query("SYSTEM RELOAD DICTIONARY ON CLUSTER 'cluster' `default.somedict`")

    for num, node in enumerate([ch1, ch2, ch3, ch4]):
        assert (
            node.query(
                "SELECT dictGetString('default.somedict', 'value', toUInt64({}))".format(
                    num
                )
            )
            == "new_key" + "\n"
        )

    ch1.query("DROP DICTIONARY default.somedict ON CLUSTER 'cluster'")

    for node in [ch1, ch2, ch3, ch4]:
        with pytest.raises(QueryRuntimeException):
            node.query("SELECT dictGetString('default.somedict', 'value', toUInt64(1))")


def test_reload_dictionary_on_cluster_preserves_initiator_database(started_cluster):
    # Regression test for `SYSTEM RELOAD DICTIONARY <bare name> ON CLUSTER`.
    #
    # ch1 acts as a gateway: it enqueues the ON CLUSTER DDL for `worker_cluster`
    # (ch2, ch3, ch4) but is not a member of it, so it does not host the target
    # dictionary. The bare dictionary name must be qualified with the initiator's
    # current database before the query is dispatched, otherwise every worker
    # re-resolves the bare name against its own `default` database and reloads the
    # wrong dictionary.
    #
    # To make the wrong-target case observable, each worker also has a decoy
    # dictionary with the same bare name `reload_dict` in `default` (its fallback
    # database), whose source carries a different value.

    for node in [ch2, ch3, ch4]:
        node.query("CREATE DATABASE IF NOT EXISTS initiator_db")

        # Real dictionary in a non-default database.
        node.query(
            "CREATE TABLE initiator_db.dict_source (key UInt64, value String) ENGINE = Memory"
        )
        node.query("INSERT INTO initiator_db.dict_source VALUES (1, 'real_old')")
        node.query(
            """
            CREATE DICTIONARY initiator_db.reload_dict (key UInt64, value String)
            PRIMARY KEY key
            LAYOUT(FLAT())
            SOURCE(CLICKHOUSE(TABLE 'dict_source' DB 'initiator_db'))
            LIFETIME(0)
            """
        )

        # Decoy dictionary with the same bare name in the worker's fallback database.
        node.query(
            "CREATE TABLE default.dict_source (key UInt64, value String) ENGINE = Memory"
        )
        node.query("INSERT INTO default.dict_source VALUES (1, 'decoy_old')")
        node.query(
            """
            CREATE DICTIONARY default.reload_dict (key UInt64, value String)
            PRIMARY KEY key
            LAYOUT(FLAT())
            SOURCE(CLICKHOUSE(TABLE 'dict_source' DB 'default'))
            LIFETIME(0)
            """
        )

        node.query("SYSTEM RELOAD DICTIONARY initiator_db.reload_dict")
        node.query("SYSTEM RELOAD DICTIONARY default.reload_dict")
        assert (
            node.query(
                "SELECT dictGetString('initiator_db.reload_dict', 'value', toUInt64(1))"
            )
            == "real_old\n"
        )
        assert (
            node.query(
                "SELECT dictGetString('default.reload_dict', 'value', toUInt64(1))"
            )
            == "decoy_old\n"
        )

        # Make both dictionaries stale so that a reload is observable.
        node.query("TRUNCATE TABLE initiator_db.dict_source")
        node.query("INSERT INTO initiator_db.dict_source VALUES (1, 'real_new')")
        node.query("TRUNCATE TABLE default.dict_source")
        node.query("INSERT INTO default.dict_source VALUES (1, 'decoy_new')")

    # The gateway knows the database but does not host the dictionary.
    ch1.query("CREATE DATABASE IF NOT EXISTS initiator_db")

    # Bare name, resolved against the initiator's current database (`initiator_db`).
    ch1.query(
        "SYSTEM RELOAD DICTIONARY reload_dict ON CLUSTER 'worker_cluster'",
        database="initiator_db",
    )

    for node in [ch2, ch3, ch4]:
        # The dictionary in the initiator's database must have been reloaded.
        assert (
            node.query(
                "SELECT dictGetString('initiator_db.reload_dict', 'value', toUInt64(1))"
            )
            == "real_new\n"
        )
        # The decoy in `default` must be untouched.
        assert (
            node.query(
                "SELECT dictGetString('default.reload_dict', 'value', toUInt64(1))"
            )
            == "decoy_old\n"
        )

    ch1.query("DROP DATABASE initiator_db SYNC")
    for node in [ch2, ch3, ch4]:
        node.query("DROP DICTIONARY initiator_db.reload_dict")
        node.query("DROP DICTIONARY default.reload_dict")
        node.query("DROP DATABASE initiator_db SYNC")
        node.query("DROP TABLE default.dict_source")
