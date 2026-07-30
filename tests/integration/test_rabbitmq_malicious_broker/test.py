"""
Tests that `ENGINE = RabbitMQ` survives a hostile broker.

`CREATE TABLE ... ENGINE = RabbitMQ` connects to the broker synchronously, so everything the
broker says during the handshake is parsed before the statement returns. A broker that
proposes a maximum frame size of zero used to make the client accept a frame of arbitrary
size while its receive buffer stayed at 4096 bytes, which is a heap out-of-bounds write.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Must match the ports allowed in configs/allowed_hosts.xml.
BROKER_PORT = 19672
TLS_BROKER_PORT = 19673

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/allowed_hosts.xml"],
    stay_alive=True,
)


def _wait_for_port(port):
    wait_condition(
        lambda: node.exec_in_container(
            ["bash", "-c", f"exec 3<>/dev/tcp/127.0.0.1/{port} && echo OK"],
            nothrow=True,
        ),
        lambda r: "OK" in r,
        max_attempts=40,
        delay=0.5,
    )


def start_malicious_broker():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "malicious_broker.py"),
        "/malicious_broker.py",
    )

    # Plain amqp broker.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /malicious_broker.py {BROKER_PORT}"
            " > /var/log/clickhouse-server/malicious_broker.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    _wait_for_port(BROKER_PORT)

    # amqps broker with a throwaway self-signed cert (the client does not verify it), so the same
    # overflow can be driven through the TLS receive path.
    node.exec_in_container(
        [
            "bash",
            "-c",
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost"
            " -keyout /broker_key.pem -out /broker_cert.pem 2>/dev/null",
        ],
        user="root",
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /malicious_broker.py {TLS_BROKER_PORT} 0 /broker_cert.pem /broker_key.pem"
            " > /var/log/clickhouse-server/malicious_broker_tls.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    _wait_for_port(TLS_BROKER_PORT)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        start_malicious_broker()
        yield cluster
    finally:
        cluster.shutdown()


def test_broker_proposing_zero_frame_max(started_cluster):
    """A broker proposing `frame_max = 0` must not be able to overflow the receive buffer."""
    error = node.query_and_get_error(
        f"""
        CREATE TABLE malicious (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://guest:guest@localhost:{BROKER_PORT}/',
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    # Reaching the connection at all means an allowed address is not rejected by the
    # `remote_url_allow_hosts` check that the test below exercises.
    assert "CANNOT_CONNECT_RABBITMQ" in error, error

    # The server has to be alive and healthy - the point of the test is that the oversized
    # frame was rejected rather than read into a buffer that is too small for it. Under a
    # sanitizer build the out-of-bounds write takes the server down and this query fails.
    assert node.query("SELECT 1") == "1\n"

    # Deterministic proof the frame-size guard actually fired (rather than the server merely
    # surviving the overflow by luck on a non-sanitizer build): the library rejects the
    # oversized frame with a `frame size exceeded` protocol error, which the handler logs.
    wait_condition(
        lambda: node.contains_in_log("frame size exceeded"),
        lambda fired: fired,
        max_attempts=20,
        delay=0.5,
    )


def test_broker_proposing_zero_frame_max_over_tls(started_cluster):
    """The same overflow driven through the TLS (amqps) receive path, which used to be unbounded.

    This is primarily a guard for the sanitizer lane: without the fix the oversized frame is read
    past the end of the TLS receive buffer and a sanitizer build aborts here, so the server must
    stay alive and answer afterwards.
    """
    error = node.query_and_get_error(
        f"""
        CREATE TABLE malicious_tls (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqps://guest:guest@localhost:{TLS_BROKER_PORT}/',
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    assert "CANNOT_CONNECT_RABBITMQ" in error, error
    assert node.query("SELECT 1") == "1\n"


def test_remote_host_filter_applies_to_rabbitmq_address(started_cluster):
    """`rabbitmq_address` must be checked against `remote_url_allow_hosts` too."""
    error = node.query_and_get_error(
        """
        CREATE TABLE filtered (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://guest:guest@not-allowed-host:5672/',
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    assert "UNACCEPTABLE_URL" in error, error
    assert "not-allowed-host:5672" in error, error


def test_remote_host_filter_not_bypassed_by_host_port(started_cluster):
    """An allowed `rabbitmq_host_port` must not smuggle a disallowed `rabbitmq_address` past the filter.

    Both settings can be given together, and `RabbitMQConnection::connectImpl` connects to
    `rabbitmq_address` (the URI) in preference to `rabbitmq_host_port`. So validating only the
    host-port form let an allowed `rabbitmq_host_port` (`localhost:19672` is in the allowlist) pair
    with a disallowed `rabbitmq_address` and still reach the unvalidated host. The address must be
    checked whenever it is set.
    """
    error = node.query_and_get_error(
        """
        CREATE TABLE both_settings (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_host_port = 'localhost:19672',
                 rabbitmq_address = 'amqp://guest:guest@not-allowed-host:5672/',
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    assert "UNACCEPTABLE_URL" in error, error
    assert "not-allowed-host:5672" in error, error
