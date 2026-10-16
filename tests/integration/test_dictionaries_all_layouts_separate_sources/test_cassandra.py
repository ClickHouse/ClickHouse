import os

import cassandra.cluster
import pytest
from cassandra.concurrent import execute_concurrent_with_args

from helpers.cluster import ClickHouseCluster
from helpers.external_sources import SourceCassandra

from .common import (
    ComplexLayoutTester,
    LAYOUTS_COMPLEX,
    LAYOUTS_RANGED,
    LAYOUTS_SIMPLE,
    RangedLayoutTester,
    SimpleLayoutTester,
)

SOURCE = None
cluster = None
node = None
simple_tester = None
complex_tester = None
ranged_tester = None
test_name = "cassandra"


def setup_module(module):
    global cluster
    global node
    global simple_tester
    global complex_tester
    global ranged_tester

    cluster = ClickHouseCluster(__file__)

    SOURCE = SourceCassandra(
        "Cassandra",
        None,
        cluster.cassandra_port,
        cluster.cassandra_host,
        cluster.cassandra_port,
        "",
        "",
    )

    simple_tester = SimpleLayoutTester(test_name)
    simple_tester.cleanup()
    simple_tester.create_dictionaries(SOURCE)

    complex_tester = ComplexLayoutTester(test_name)
    complex_tester.create_dictionaries(SOURCE)

    ranged_tester = RangedLayoutTester(test_name)
    ranged_tester.create_dictionaries(SOURCE)
    # Since that all .xml configs were created

    main_configs = []
    main_configs.append(os.path.join("configs", "disable_ssl_verification.xml"))

    dictionaries = simple_tester.list_dictionaries()

    node = cluster.add_instance(
        "cass_node",
        main_configs=main_configs,
        dictionaries=dictionaries,
        with_cassandra=True,
    )


def teardown_module(module):
    simple_tester.cleanup()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        simple_tester.prepare(cluster)
        complex_tester.prepare(cluster)
        ranged_tester.prepare(cluster)

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple(started_cluster, layout_name):
    simple_tester.execute(layout_name, node)


@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_COMPLEX))
def test_complex(started_cluster, layout_name):
    complex_tester.execute(layout_name, node)


@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(started_cluster, layout_name):
    ranged_tester.execute(layout_name, node)


PAGING_ROWS = 20000


@pytest.fixture(scope="module")
def paging_table(started_cluster):
    client = cassandra.cluster.Cluster(
        [started_cluster.cassandra_ip], port=started_cluster.cassandra_port
    )
    session = client.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS paging WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("DROP TABLE IF EXISTS paging.t")
    session.execute(
        "CREATE TABLE paging.t (id bigint PRIMARY KEY, value text, flag boolean, ts timestamp)"
    )
    insert = session.prepare(
        "INSERT INTO paging.t (id, value, flag, ts) VALUES (?, ?, ?, ?)"
    )
    execute_concurrent_with_args(
        session,
        insert,
        [(i, f"value_{i}", i % 2 == 0, i * 1000) for i in range(PAGING_ROWS)],
        concurrency=100,
        raise_on_first_error=True,
    )
    client.shutdown()


@pytest.mark.parametrize(
    "secure, compression", [(0, "none"), (0, "snappy"), (1, "none"), (1, "lz4")]
)
def test_paging_and_hosts(started_cluster, paging_table, secure, compression):
    """
    Load a table that does not fit into one page of the result,
    with a list of hosts where the first one does not exist.
    """
    node.query(
        f"""
        CREATE DICTIONARY paging_dict (id UInt64, value String, flag UInt8, ts DateTime('UTC'))
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host 'nonexistent_cassandra_host, {started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            secure {secure}
            compression '{compression}'
            keyspace 'paging'
            column_family 't'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )

    try:
        assert (
            node.query(
                "SELECT count(), sum(id), sum(length(value)), sum(flag), sum(toUnixTimestamp(ts)) FROM paging_dict"
            )
            == node.query(
                f"SELECT count(), sum(number), sum(length('value_' || toString(number))), sum(number % 2 = 0), sum(number) FROM numbers({PAGING_ROWS})"
            )
        )
        assert node.query("SELECT dictGet('paging_dict', 'value', 12345::UInt64)") == "value_12345\n"
    finally:
        node.query("DROP DICTIONARY paging_dict")


def test_errors(started_cluster, paging_table):
    node.query(
        f"""
        CREATE DICTIONARY missing_table_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            keyspace 'paging'
            column_family 'missing_table'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        error = node.query_and_get_error("SELECT * FROM missing_table_dict")
        assert "CASSANDRA_INTERNAL_ERROR" in error
        assert "missing_table" in error
    finally:
        node.query("DROP DICTIONARY missing_table_dict")

    node.query(
        """
        CREATE DICTIONARY unreachable_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host 'nonexistent_cassandra_host'
            keyspace 'paging'
            column_family 't'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        error = node.query_and_get_error("SELECT * FROM unreachable_dict")
        assert "ALL_CONNECTION_TRIES_FAILED" in error
    finally:
        node.query("DROP DICTIONARY unreachable_dict")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, value FROM paging.t",
        "SELECT id, value FROM paging.t WHERE {condition}",
        "SELECT id, value FROM paging.t WHERE {condition} AND value > \\'\\' ALLOW FILTERING",
        "SELECT id, value FROM paging.t WHERE value > \\'\\' AND {condition} ALLOW FILTERING;",
    ],
)
def test_custom_query_load_all(started_cluster, paging_table, query):
    """
    A full load with a custom query, with and without the `{condition}` placeholder,
    which is removed together with `WHERE` or `AND` for a full load, because CQL has no tautologies.
    """
    node.query(
        f"""
        CREATE DICTIONARY query_all_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            query '{query}'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        assert node.query(
            "SELECT count(), sum(id), sum(length(value)) FROM query_all_dict"
        ) == node.query(
            f"SELECT count(), sum(number), sum(length('value_' || toString(number))) FROM numbers({PAGING_ROWS})"
        )
    finally:
        node.query("DROP DICTIONARY query_all_dict")


@pytest.mark.parametrize("layout", ["DIRECT", "COMPLEX_KEY_DIRECT"])
def test_custom_query_selective_load(started_cluster, paging_table, layout):
    """
    A selective load with a custom query, where `{condition}` is replaced with
    the condition on the requested keys (`loadIds` for `DIRECT`, `loadKeys` for `COMPLEX_KEY_DIRECT`).
    """
    node.query(
        f"""
        CREATE DICTIONARY query_selective_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            query 'SELECT id, value FROM paging.t WHERE {{condition}}'))
        LAYOUT({layout}())
        """
    )
    try:
        key = "12345::UInt64" if layout == "DIRECT" else "tuple(12345::UInt64)"
        missing_key = (
            f"{PAGING_ROWS}::UInt64"
            if layout == "DIRECT"
            else f"tuple({PAGING_ROWS}::UInt64)"
        )
        assert (
            node.query(f"SELECT dictGet('query_selective_dict', 'value', {key})")
            == "value_12345\n"
        )
        assert (
            node.query(
                f"SELECT dictGetOrDefault('query_selective_dict', 'value', {missing_key}, 'missing')"
            )
            == "missing\n"
        )
    finally:
        node.query("DROP DICTIONARY query_selective_dict")


def test_custom_query_selective_load_without_condition(started_cluster, paging_table):
    """
    CQL does not support subqueries, so loading keys with a custom query requires `{condition}`.
    """
    node.query(
        f"""
        CREATE DICTIONARY query_no_condition_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            query 'SELECT id, value FROM paging.t'))
        LAYOUT(COMPLEX_KEY_DIRECT())
        """
    )
    try:
        error = node.query_and_get_error(
            "SELECT dictGet('query_no_condition_dict', 'value', tuple(1::UInt64))"
        )
        assert "UNSUPPORTED_METHOD" in error
        assert "{condition}" in error
    finally:
        node.query("DROP DICTIONARY query_no_condition_dict")


def test_custom_query_column_names(started_cluster, paging_table):
    """
    The columns returned by a custom query are matched by position, so their names must
    match the dictionary columns, otherwise values could go into wrong attributes of the same type.
    """
    node.query(
        f"""
        CREATE DICTIONARY query_alias_dict (key UInt64, value String)
        PRIMARY KEY key
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            query 'SELECT id AS key, value FROM paging.t'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        assert node.query("SELECT dictGet('query_alias_dict', 'value', 12345::UInt64)") == "value_12345\n"
    finally:
        node.query("DROP DICTIONARY query_alias_dict")

    node.query(
        f"""
        CREATE DICTIONARY query_wrong_names_dict (id UInt64, a String, b String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            query 'SELECT id, value AS b, value AS a FROM paging.t'))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        error = node.query_and_get_error("SELECT * FROM query_wrong_names_dict")
        assert "NO_SUCH_COLUMN_IN_TABLE" in error
        assert "returned column `b` at position 2, expected `a`" in error
    finally:
        node.query("DROP DICTIONARY query_wrong_names_dict")


@pytest.mark.parametrize(
    "settings",
    [
        "query 'SELECT id, value FROM paging.t' where 'id > 0'",
        "query 'SELECT id, value FROM paging.t' keyspace 'paging' column_family 't'",
        "keyspace 'paging'",
        "column_family 't'",
    ],
)
def test_invalid_configuration(started_cluster, settings):
    node.query(
        f"""
        CREATE DICTIONARY invalid_config_dict (id UInt64, value String)
        PRIMARY KEY id
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            {settings}))
        LAYOUT(HASHED())
        LIFETIME(0)
        """
    )
    try:
        error = node.query_and_get_error("SELECT * FROM invalid_config_dict")
        assert "INVALID_CONFIG_PARAMETER" in error
    finally:
        node.query("DROP DICTIONARY invalid_config_dict")


@pytest.mark.parametrize("partition_key_prefix", [0, 3])
def test_invalid_partition_key_prefix(started_cluster, partition_key_prefix):
    node.query(
        f"""
        CREATE DICTIONARY invalid_prefix_dict (k1 UInt64, k2 String, value String)
        PRIMARY KEY k1, k2
        SOURCE(CASSANDRA(
            host '{started_cluster.cassandra_host}'
            port {started_cluster.cassandra_port}
            keyspace 'paging'
            column_family 't'
            partition_key_prefix {partition_key_prefix}))
        LAYOUT(COMPLEX_KEY_DIRECT())
        """
    )
    try:
        error = node.query_and_get_error(
            "SELECT dictGet('invalid_prefix_dict', 'value', (toUInt64(1), 'a'))"
        )
        assert "INVALID_CONFIG_PARAMETER" in error
    finally:
        node.query("DROP DICTIONARY invalid_prefix_dict")
