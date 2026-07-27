import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)
# The XML dictionary is deployed only to the nodes of `worker_cluster`, so that ch1 can
# initiate an `ON CLUSTER` query for a dictionary it does not know anything about.
ch2 = cluster.add_instance(
    "ch2",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    dictionaries=["configs/dictionaries/xml_dict.xml"],
    with_zookeeper=True,
)
ch3 = cluster.add_instance(
    "ch3",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    dictionaries=["configs/dictionaries/xml_dict.xml"],
    with_zookeeper=True,
)
ch4 = cluster.add_instance(
    "ch4",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    dictionaries=["configs/dictionaries/xml_dict.xml"],
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
    # ch1 initiates the query for `worker_cluster` (ch2, ch3, ch4) without being a member
    # of it, so it never executes the query itself. The bare dictionary name must be
    # qualified with the initiator's current database before the query is dispatched,
    # otherwise every worker re-resolves the bare name against its own current database,
    # which for a query arriving over the DDL queue is `default`.
    #
    # To make the wrong-target case observable, each worker also has a decoy dictionary
    # with the same bare name `reload_dict` in `default`, whose source carries a
    # different value.

    for node in [ch1, ch2, ch3, ch4]:
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

    for node in [ch2, ch3, ch4]:
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

    for node in [ch1, ch2, ch3, ch4]:
        node.query("DROP DATABASE initiator_db SYNC")
    for node in [ch2, ch3, ch4]:
        node.query("DROP DICTIONARY default.reload_dict")
        node.query("DROP TABLE default.dict_source")


def test_reload_xml_dictionary_on_cluster_from_non_hosting_initiator(started_cluster):
    # An XML dictionary is referenced by its bare name, without a database. Qualifying such
    # a name with the initiator's current database would make it unresolvable on the
    # workers, or would make it resolve to a same-named dictionary created by DDL.
    #
    # ch1 initiates the query for `worker_cluster` (ch2, ch3, ch4) without being a member
    # of it, and `xml_only_dict` is deployed only to those workers, so ch1 cannot resolve
    # the name at all and must forward it unchanged.
    #
    # To make the wrong-target case observable, each worker also has a decoy dictionary
    # created by DDL, named `default.xml_only_dict`, whose source carries a different value.

    for node in [ch2, ch3, ch4]:
        node.query(
            "CREATE TABLE default.xml_dict_source (key UInt64, value String) ENGINE = Memory"
        )
        node.query("INSERT INTO default.xml_dict_source VALUES (1, 'xml_old')")

        node.query(
            "CREATE TABLE default.decoy_dict_source (key UInt64, value String) ENGINE = Memory"
        )
        node.query("INSERT INTO default.decoy_dict_source VALUES (1, 'decoy_old')")
        node.query(
            """
            CREATE DICTIONARY default.xml_only_dict (key UInt64, value String)
            PRIMARY KEY key
            LAYOUT(FLAT())
            SOURCE(CLICKHOUSE(TABLE 'decoy_dict_source' DB 'default'))
            LIFETIME(0)
            """
        )

        # The XML dictionary is resolved by its bare name, the decoy one only when qualified.
        node.query("SYSTEM RELOAD DICTIONARY xml_only_dict")
        node.query("SYSTEM RELOAD DICTIONARY default.xml_only_dict")
        assert (
            node.query("SELECT dictGetString('xml_only_dict', 'value', toUInt64(1))")
            == "xml_old\n"
        )
        assert (
            node.query(
                "SELECT dictGetString('default.xml_only_dict', 'value', toUInt64(1))"
            )
            == "decoy_old\n"
        )

        # Make both dictionaries stale so that a reload is observable.
        node.query("TRUNCATE TABLE default.xml_dict_source")
        node.query("INSERT INTO default.xml_dict_source VALUES (1, 'xml_new')")
        node.query("TRUNCATE TABLE default.decoy_dict_source")
        node.query("INSERT INTO default.decoy_dict_source VALUES (1, 'decoy_new')")

    # The initiator has neither the XML dictionary nor a dictionary with that name in its
    # current database, so the bare name must reach the workers unchanged.
    ch1.query("SYSTEM RELOAD DICTIONARY xml_only_dict ON CLUSTER 'worker_cluster'")

    for node in [ch2, ch3, ch4]:
        # The XML dictionary must have been reloaded.
        assert (
            node.query("SELECT dictGetString('xml_only_dict', 'value', toUInt64(1))")
            == "xml_new\n"
        )
        # The same-named dictionary created by DDL must be untouched.
        assert (
            node.query(
                "SELECT dictGetString('default.xml_only_dict', 'value', toUInt64(1))"
            )
            == "decoy_old\n"
        )

    for node in [ch2, ch3, ch4]:
        node.query("DROP DICTIONARY default.xml_only_dict")
        node.query("DROP TABLE default.decoy_dict_source")
        node.query("DROP TABLE default.xml_dict_source")
