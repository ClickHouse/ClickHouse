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
