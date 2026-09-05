import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    dictionaries=["configs/dictionaries/issue_114322_xml_only.xml"],
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
coordinator = cluster.add_instance(
    "coordinator",
    main_configs=["configs/config.d/clusters.xml", "configs/config.d/ddl.xml"],
    with_zookeeper=True,
)

DICTIONARY_DB = "issue_114322"
DICTIONARY_NAME = "issue_114322_dict"
DOTTED_DICTIONARY_NAME = "issue_114322_dict.with.dot"
XML_ONLY_DICTIONARY_NAME = "issue_114322_xml_only"


def get_issue_114322_dictionary_status_query(name=DICTIONARY_NAME):
    return """
    SELECT database, status
    FROM system.dictionaries
    WHERE name = '{name}' AND database IN ('default', '{database}')
    ORDER BY database
""".format(name=name, database=DICTIONARY_DB)


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

    ch1.query("SYSTEM RELOAD DICTIONARY ON CLUSTER 'cluster' default.somedict")

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


def drop_issue_114322_dictionaries():
    for node in (ch1, coordinator):
        node.query(f"DROP DICTIONARY IF EXISTS default.{DICTIONARY_NAME}")
        node.query(f"DROP DICTIONARY IF EXISTS default.`{DOTTED_DICTIONARY_NAME}`")
        node.query(f"DROP DATABASE IF EXISTS {DICTIONARY_DB} SYNC")


def create_issue_114322_database():
    ch1.query(f"CREATE DATABASE IF NOT EXISTS {DICTIONARY_DB}")
    coordinator.query(f"CREATE DATABASE IF NOT EXISTS {DICTIONARY_DB}")


def create_issue_114322_dictionary(node, database, name=DICTIONARY_NAME):
    node.query(
        f"""
        CREATE DICTIONARY {database}.`{name}` (k UInt64, v String)
        PRIMARY KEY k
        SOURCE(NULL())
        LAYOUT(FLAT())
        LIFETIME(0)
        """
    )


def unload_issue_114322_dictionaries(node, include_default=True):
    node.query(f"SYSTEM UNLOAD DICTIONARY {DICTIONARY_DB}.{DICTIONARY_NAME}")
    if include_default:
        node.query(f"SYSTEM UNLOAD DICTIONARY default.{DICTIONARY_NAME}")


def reload_issue_114322_dictionaries(node, include_default=True):
    node.query(f"SYSTEM RELOAD DICTIONARY {DICTIONARY_DB}.{DICTIONARY_NAME}")
    if include_default:
        node.query(f"SYSTEM RELOAD DICTIONARY default.{DICTIONARY_NAME}")


def prepare_issue_114322_dictionaries(include_default=True):
    drop_issue_114322_dictionaries()
    create_issue_114322_database()
    create_issue_114322_dictionary(ch1, DICTIONARY_DB)
    if include_default:
        create_issue_114322_dictionary(ch1, "default")


def assert_issue_114322_dictionary_statuses(expected, name=DICTIONARY_NAME):
    assert_eq_with_retry(
        ch1, get_issue_114322_dictionary_status_query(name), expected
    )


@pytest.mark.parametrize(
    "initiator, cluster_name",
    [
        pytest.param(ch1, "one_node", id="initiator_is_cluster_member"),
        pytest.param(
            coordinator, "workers_only", id="coordinator_is_not_cluster_member"
        ),
    ],
)
def test_reload_dictionary_on_cluster_uses_initiator_database_for_bare_name(
    started_cluster, initiator, cluster_name
):
    try:
        prepare_issue_114322_dictionaries()
        unload_issue_114322_dictionaries(ch1)
        assert_issue_114322_dictionary_statuses(
            f"default\tNOT_LOADED\n{DICTIONARY_DB}\tNOT_LOADED\n"
        )

        initiator.query(
            f"SYSTEM RELOAD DICTIONARY {DICTIONARY_NAME} ON CLUSTER '{cluster_name}'",
            database=DICTIONARY_DB,
        )

        assert_issue_114322_dictionary_statuses(
            f"default\tNOT_LOADED\n{DICTIONARY_DB}\tLOADED\n"
        )
    finally:
        drop_issue_114322_dictionaries()


@pytest.mark.parametrize(
    "initiator, cluster_name",
    [
        pytest.param(ch1, "one_node", id="initiator_is_cluster_member"),
        pytest.param(
            coordinator, "workers_only", id="coordinator_is_not_cluster_member"
        ),
    ],
)
def test_unload_dictionary_on_cluster_uses_initiator_database_for_bare_name(
    started_cluster, initiator, cluster_name
):
    try:
        prepare_issue_114322_dictionaries()
        reload_issue_114322_dictionaries(ch1)
        assert_issue_114322_dictionary_statuses(
            f"default\tLOADED\n{DICTIONARY_DB}\tLOADED\n"
        )

        initiator.query(
            f"SYSTEM UNLOAD DICTIONARY {DICTIONARY_NAME} ON CLUSTER '{cluster_name}'",
            database=DICTIONARY_DB,
        )

        assert_issue_114322_dictionary_statuses(
            f"default\tLOADED\n{DICTIONARY_DB}\tNOT_LOADED\n"
        )
    finally:
        drop_issue_114322_dictionaries()


def test_reload_dictionary_on_cluster_uses_initiator_database_without_default_decoy(
    started_cluster,
):
    try:
        prepare_issue_114322_dictionaries(include_default=False)
        unload_issue_114322_dictionaries(ch1, include_default=False)
        assert_issue_114322_dictionary_statuses(f"{DICTIONARY_DB}\tNOT_LOADED\n")

        coordinator.query(
            f"SYSTEM RELOAD DICTIONARY {DICTIONARY_NAME} ON CLUSTER 'workers_only'",
            database=DICTIONARY_DB,
        )

        assert_issue_114322_dictionary_statuses(f"{DICTIONARY_DB}\tLOADED\n")
    finally:
        drop_issue_114322_dictionaries()


def test_reload_dictionary_on_cluster_uses_initiator_database_for_dotted_string_literal_name(
    started_cluster,
):
    try:
        drop_issue_114322_dictionaries()
        create_issue_114322_database()
        create_issue_114322_dictionary(ch1, DICTIONARY_DB, DOTTED_DICTIONARY_NAME)
        create_issue_114322_dictionary(ch1, "default", DOTTED_DICTIONARY_NAME)
        ch1.query(
            f"SYSTEM UNLOAD DICTIONARY {DICTIONARY_DB}.`{DOTTED_DICTIONARY_NAME}`"
        )
        ch1.query(f"SYSTEM UNLOAD DICTIONARY default.`{DOTTED_DICTIONARY_NAME}`")
        assert_issue_114322_dictionary_statuses(
            f"default\tNOT_LOADED\n{DICTIONARY_DB}\tNOT_LOADED\n",
            DOTTED_DICTIONARY_NAME,
        )

        coordinator.query(
            f"SYSTEM RELOAD DICTIONARY '{DOTTED_DICTIONARY_NAME}' ON CLUSTER 'workers_only'",
            database=DICTIONARY_DB,
        )

        assert_issue_114322_dictionary_statuses(
            f"default\tNOT_LOADED\n{DICTIONARY_DB}\tLOADED\n",
            DOTTED_DICTIONARY_NAME,
        )
    finally:
        drop_issue_114322_dictionaries()


def test_qualified_dictionary_on_cluster_keeps_explicit_database(
    started_cluster,
):
    try:
        prepare_issue_114322_dictionaries()
        unload_issue_114322_dictionaries(ch1)
        assert_issue_114322_dictionary_statuses(
            f"default\tNOT_LOADED\n{DICTIONARY_DB}\tNOT_LOADED\n"
        )

        coordinator.query(
            f"SYSTEM RELOAD DICTIONARY default.{DICTIONARY_NAME} ON CLUSTER 'workers_only'",
            database=DICTIONARY_DB,
        )

        assert_issue_114322_dictionary_statuses(
            f"default\tLOADED\n{DICTIONARY_DB}\tNOT_LOADED\n"
        )
    finally:
        drop_issue_114322_dictionaries()


def test_reload_xml_only_dictionary_on_cluster_uses_qualified_database_name(
    started_cluster,
):
    try:
        drop_issue_114322_dictionaries()
        create_issue_114322_database()

        with pytest.raises(QueryRuntimeException) as exc:
            coordinator.query(
                f"SYSTEM RELOAD DICTIONARY {XML_ONLY_DICTIONARY_NAME} ON CLUSTER 'workers_only'",
                database=DICTIONARY_DB,
            )

        exception = str(exc.value)
        assert "Dictionary" in exception
        assert f"{DICTIONARY_DB}.{XML_ONLY_DICTIONARY_NAME}" in exception
        assert "BAD_ARGUMENTS" in exception or "Code: 36" in exception
    finally:
        drop_issue_114322_dictionaries()


def test_reload_dictionary_on_cluster_uses_shard_default_database_when_configured(
    started_cluster,
):
    try:
        prepare_issue_114322_dictionaries(include_default=False)
        unload_issue_114322_dictionaries(ch1, include_default=False)
        assert_issue_114322_dictionary_statuses(f"{DICTIONARY_DB}\tNOT_LOADED\n")

        coordinator.query(
            f"SYSTEM RELOAD DICTIONARY {DICTIONARY_NAME} ON CLUSTER 'workers_with_default_db'",
            database="default",
        )

        assert_issue_114322_dictionary_statuses(f"{DICTIONARY_DB}\tLOADED\n")
    finally:
        drop_issue_114322_dictionaries()
