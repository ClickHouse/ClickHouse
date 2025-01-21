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
