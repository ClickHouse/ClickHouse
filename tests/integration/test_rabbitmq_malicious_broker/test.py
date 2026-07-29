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

# Must match the port allowed in configs/allowed_hosts.xml.
BROKER_PORT = 19672

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/allowed_hosts.xml"],
    stay_alive=True,
)


def start_malicious_broker():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "malicious_broker.py"),
        "/malicious_broker.py",
    )
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
    wait_condition(
        lambda: node.exec_in_container(
            [
                "bash",
                "-c",
                f"exec 3<>/dev/tcp/127.0.0.1/{BROKER_PORT} && echo OK",
            ],
            nothrow=True,
        ),
        lambda r: "OK" in r,
        max_attempts=40,
        delay=0.5,
    )


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
