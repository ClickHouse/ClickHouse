"""
Tests that the `Kafka` and `NATS` table engines respect `remote_url_allow_hosts`.

Both engines used to hand the user-supplied broker addresses (`kafka_broker_list`,
`nats_url`, `nats_server_list`) to the client library without consulting
`RemoteHostFilter`, so `CREATE TABLE` opened outbound TCP connections to hosts the
operator had explicitly forbidden. `RabbitMQ`, the sibling engine, rejects the same
addresses at `CREATE` with `UNACCEPTABLE_URL`.

No broker runs in this test: a forbidden address must be rejected before any
connection attempt, and an allowed one must get past the filter (and then fail to
connect, for the engine which connects at DDL time).
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/allowed_hosts.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_kafka(broker_list):
    return f"""
        CREATE TABLE kafka_filtered (key UInt64, value UInt64)
        ENGINE = Kafka
        SETTINGS kafka_broker_list = '{broker_list}',
                 kafka_topic_list = 'topic',
                 kafka_group_name = 'group',
                 kafka_format = 'JSONEachRow'
        """


def create_nats(settings):
    return f"""
        CREATE TABLE nats_filtered (key UInt64, value UInt64)
        ENGINE = NATS
        SETTINGS {settings},
                 nats_subjects = 'subject',
                 nats_format = 'JSONEachRow'
        """


def test_kafka_broker_rejected(started_cluster):
    error = node.query_and_get_error(create_kafka("localhost:9093"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9093" in error, error


def test_kafka_broker_list_checked_element_wise(started_cluster):
    """One allowed broker must not smuggle a forbidden one past the filter."""
    error = node.query_and_get_error(create_kafka("localhost:19092,localhost:9093"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9093" in error, error


def test_kafka_scheme_prefix_is_stripped(started_cluster):
    """librdkafka accepts `scheme://host:port` entries; the filter must see host:port."""
    error = node.query_and_get_error(create_kafka("PLAINTEXT://localhost:9093"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9093" in error, error


def test_kafka_default_port(started_cluster):
    """A bare host must be checked with librdkafka's default port 9092."""
    error = node.query_and_get_error(create_kafka("localhost"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9092" in error, error


def test_kafka_host_allowed_for_any_port(started_cluster):
    """A bare `<host>` allowlist entry allows the host on any port, as for other engines."""
    node.query(create_kafka("allowed-any-port:9095"))
    node.query("DROP TABLE kafka_filtered SYNC")


def test_kafka_allowed_broker_passes(started_cluster):
    """`CREATE` must succeed for an allowed broker (librdkafka connects in background)."""
    node.query(create_kafka("localhost:19092"))
    node.query("DROP TABLE kafka_filtered SYNC")


def test_nats_url_rejected(started_cluster):
    error = node.query_and_get_error(create_nats("nats_url = 'localhost:9999'"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9999" in error, error


def test_nats_url_scheme_and_credentials_are_stripped(started_cluster):
    """libnats accepts `nats://user:password@host:port`; the filter must see host:port."""
    error = node.query_and_get_error(
        create_nats("nats_url = 'nats://user:password@localhost:9999'")
    )
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9999" in error, error


def test_nats_server_list_checked_element_wise(started_cluster):
    """One allowed server must not smuggle a forbidden one past the filter."""
    error = node.query_and_get_error(
        create_nats("nats_server_list = 'localhost:14222,localhost:9999'")
    )
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9999" in error, error


def test_nats_default_port(started_cluster):
    """A bare host must be checked with libnats' default port 4222."""
    error = node.query_and_get_error(create_nats("nats_url = 'localhost'"))
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:4222" in error, error


def test_nats_allowed_url_passes_filter(started_cluster):
    """An allowed address must get past the filter and reach the connection attempt.

    `NATS` connects at `CREATE` time and nothing listens on the allowed port, so the
    statement fails - but with a connection error, not `UNACCEPTABLE_URL`.
    """
    error = node.query_and_get_error(
        create_nats(
            "nats_url = 'localhost:14222', nats_startup_connect_tries = 1"
        )
    )
    assert "UNACCEPTABLE_URL" not in error, error
    assert "CANNOT_CONNECT_NATS" in error, error
