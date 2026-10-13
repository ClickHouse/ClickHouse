"""
Tests that the `Kafka` and `NATS` table engines respect `remote_url_allow_hosts`.

Both engines used to hand the user-supplied broker addresses (`kafka_broker_list`,
`nats_url`, `nats_server_list`) to the client library without consulting
`RemoteHostFilter`, so `CREATE TABLE` opened outbound TCP connections to hosts the
operator had explicitly forbidden. `RabbitMQ`, the sibling engine, rejects the same
addresses at `CREATE` with `UNACCEPTABLE_URL`.

The addresses are parsed, validated element-wise, and rebuilt before they reach the
client library, so an entry whose re-parse by the library could disagree with the
validated form (a path, an empty host, a NUL, ...) is rejected with `BAD_ARGUMENTS`
instead of being repaired.

No broker runs in this test: a forbidden or malformed address must be rejected before
any connection attempt, and an allowed one must get past the filter (and then fail to
connect, for the engine which connects at DDL time).

Each case is a triplet: the engine input, the expected error code (`None` means the
table must be created), and an optional string the error message must contain.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/allowed_hosts.xml"],
)
# The <kafka> section of the server configuration (and named collections, loaded by the same code)
# can override the broker list librdkafka receives, behind the back of the validated
# `kafka_broker_list` setting. This instance carries such an override to a forbidden host.
node_kafka_override = cluster.add_instance(
    "node_kafka_override",
    main_configs=["configs/allowed_hosts.xml", "configs/kafka_bootstrap_override.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# (kafka_broker_list value, expected error code or None for success, required error message part)
KAFKA_CASES = [
    pytest.param("localhost:9093", "UNACCEPTABLE_URL", "localhost:9093", id="forbidden_broker"),
    # One allowed broker must not smuggle a forbidden one past the filter.
    pytest.param("localhost:19092,localhost:9093", "UNACCEPTABLE_URL", "localhost:9093", id="element_wise"),
    # librdkafka accepts `scheme://host:port` entries; the filter must see host:port.
    pytest.param("PLAINTEXT://localhost:9093", "UNACCEPTABLE_URL", "localhost:9093", id="scheme_prefix"),
    # A bare host must be checked with librdkafka's default port 9092.
    pytest.param("localhost", "UNACCEPTABLE_URL", "localhost:9092", id="default_port"),
    # A bare `<host>` allowlist entry allows the host on any port, as for other engines.
    pytest.param("allowed-any-port:9095", None, None, id="host_allowed_for_any_port"),
    pytest.param("localhost:19092", None, None, id="allowed_broker"),
    # librdkafka cuts an entry at the first `/` after `scheme://`. Without the strict parse, the
    # filter would see the host `evil.com/.allowed.example.com`, the `host_regexp` allowlist entry
    # would fully match it, and librdkafka would dial `evil.com`.
    pytest.param(
        "PLAINTEXT://evil.com/.allowed.example.com", "BAD_ARGUMENTS", "Unexpected character '/'",
        id="path_does_not_bypass_host_regexp",
    ),
    # librdkafka splits the broker list on spaces too. Without doing the same, the filter would see
    # the single host `evil.com .allowed.example.com`, the `host_regexp` allowlist entry would fully
    # match it, and librdkafka would dial `evil.com`.
    pytest.param("evil.com .allowed.example.com", "UNACCEPTABLE_URL", "evil.com:9092", id="space_separator"),
    # librdkafka substitutes `localhost` for an empty host.
    pytest.param(":9092", "BAD_ARGUMENTS", "Empty host", id="empty_host"),
    # The broker list reaches librdkafka as a C string, so a NUL must not truncate it after the check.
    pytest.param("evil.com\\0.allowed.example.com", "BAD_ARGUMENTS", "NUL", id="nul"),
    # The validated entries are rebuilt and rejoined; the original text never reaches librdkafka.
    pytest.param("localhost:19092 ,  localhost:19092", None, None, id="canonicalized"),
    # An allowed host by the `host_regexp` allowlist entry.
    pytest.param("sub.allowed.example.com", None, None, id="host_regexp_allowed"),
]

# (NATS settings, expected error code or None for success, required error message part)
NATS_CASES = [
    pytest.param("nats_url = 'localhost:9999'", "UNACCEPTABLE_URL", "localhost:9999", id="forbidden_url"),
    # libnats accepts `nats://user:password@host:port`; the filter must see host:port.
    pytest.param(
        "nats_url = 'nats://user:password@localhost:9999'", "UNACCEPTABLE_URL", "localhost:9999",
        id="scheme_and_credentials",
    ),
    # One allowed server must not smuggle a forbidden one past the filter.
    pytest.param(
        "nats_server_list = 'localhost:14222,localhost:9999'", "UNACCEPTABLE_URL", "localhost:9999",
        id="server_list_element_wise",
    ),
    # A bare host must be checked with libnats' default port 4222.
    pytest.param("nats_url = 'localhost'", "UNACCEPTABLE_URL", "localhost:4222", id="default_port"),
    # An allowed address must get past the filter and reach the connection attempt. `NATS` connects
    # at `CREATE` time and nothing listens on the allowed port, so the statement fails - but with a
    # connection error, not `UNACCEPTABLE_URL`.
    pytest.param(
        "nats_url = 'localhost:14222', nats_startup_connect_tries = 1", "CANNOT_CONNECT_NATS", None,
        id="allowed_url_passes_filter",
    ),
    # libnats allows a `/path` after the port, so a path must not reach the filter as part of the host.
    pytest.param(
        "nats_url = 'nats://evil.com:4222/x.allowed.example.com'", "BAD_ARGUMENTS", "Unexpected character '/'",
        id="path_does_not_bypass_host_regexp",
    ),
    # libnats substitutes `localhost` for an empty host.
    pytest.param("nats_url = 'nats://:4222'", "BAD_ARGUMENTS", "Empty host", id="empty_host"),
    # The URL reaches libnats as a C string, so a NUL must not truncate it after the check.
    pytest.param("nats_url = 'evil.com\\0.allowed.example.com'", "BAD_ARGUMENTS", "NUL", id="nul"),
    # The credentials must survive the rebuild of the validated address.
    pytest.param(
        "nats_url = 'nats://user:password@localhost:14222', nats_startup_connect_tries = 1",
        "CANNOT_CONNECT_NATS", None,
        id="credentials_preserved",
    ),
]


def check(query, expected_error, message_part):
    if expected_error is None:
        node.query(query)
        return

    error = node.query_and_get_error(query)
    assert expected_error in error, error
    if message_part is not None:
        assert message_part in error, error


@pytest.mark.parametrize("broker_list, expected_error, message_part", KAFKA_CASES)
def test_kafka(started_cluster, broker_list, expected_error, message_part):
    node.query("DROP TABLE IF EXISTS kafka_filtered SYNC")
    check(
        f"""
        CREATE TABLE kafka_filtered (key UInt64, value UInt64)
        ENGINE = Kafka
        SETTINGS kafka_broker_list = '{broker_list}',
                 kafka_topic_list = 'topic',
                 kafka_group_name = 'group',
                 kafka_format = 'JSONEachRow'
        """,
        expected_error,
        message_part,
    )


def test_kafka_config_override_is_validated(started_cluster):
    """A broker list supplied by the server configuration must be validated too.

    `getConsumerConfiguration` seeds `metadata.broker.list` from the validated
    `kafka_broker_list`, but the `<kafka>` section of the server configuration (or a named
    collection) is merged afterwards and can override it - here through the
    `bootstrap.servers` alias. The merged value is validated again when the consumer is
    created, so the `CREATE` (which sees only the allowed setting) succeeds and the read
    fails on the forbidden override.
    """
    node_kafka_override.query(
        """
        CREATE TABLE kafka_override (key UInt64, value UInt64)
        ENGINE = Kafka
        SETTINGS kafka_broker_list = 'localhost:19092',
                 kafka_topic_list = 'topic',
                 kafka_group_name = 'group',
                 kafka_format = 'JSONEachRow'
        """
    )
    error = node_kafka_override.query_and_get_error(
        "SELECT * FROM kafka_override LIMIT 1"
        " SETTINGS stream_like_engine_allow_direct_select = 1"
    )
    assert "UNACCEPTABLE_URL" in error, error
    assert "localhost:9999" in error, error
    node_kafka_override.query("DROP TABLE kafka_override SYNC")


@pytest.mark.parametrize("settings, expected_error, message_part", NATS_CASES)
def test_nats(started_cluster, settings, expected_error, message_part):
    node.query("DROP TABLE IF EXISTS nats_filtered SYNC")
    check(
        f"""
        CREATE TABLE nats_filtered (key UInt64, value UInt64)
        ENGINE = NATS
        SETTINGS {settings},
                 nats_subjects = 'subject',
                 nats_format = 'JSONEachRow'
        """,
        expected_error,
        message_part,
    )
