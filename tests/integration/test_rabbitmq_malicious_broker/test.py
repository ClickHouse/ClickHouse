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
HUGE_FRAME_MAX_BROKER_PORT = 19674

# The client must clamp the broker's frame_max proposal to this range and reply with the
# clamped value in `Connection.TuneOk` (MIN_FRAME_SIZE / MAX_FRAME_SIZE in AMQP-CPP).
MIN_FRAME_SIZE = 4096
MAX_FRAME_SIZE = 128 * 1024 * 1024

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

    # Plain amqp broker proposing an absurdly large frame_max, to cover the upper clamp.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /malicious_broker.py {HUGE_FRAME_MAX_BROKER_PORT} {2**32 - 1}"
            " > /var/log/clickhouse-server/malicious_broker_huge.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    _wait_for_port(HUGE_FRAME_MAX_BROKER_PORT)


def _assert_negotiated_frame_max(broker_log, expected):
    """The broker logs the frame_max from the client's `Connection.TuneOk` reply."""
    wait_condition(
        lambda: node.exec_in_container(
            [
                "bash",
                "-c",
                f"grep -a 'client TuneOk frame_max=' /var/log/clickhouse-server/{broker_log}"
                " | tail -1",
            ],
            nothrow=True,
        ),
        lambda reply: f"frame_max={expected}" in reply,
        max_attempts=20,
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

    # Deterministic proof the frame-size guard actually fired (rather than the server merely
    # surviving the overflow by luck on a non-sanitizer build): the library rejects the
    # oversized frame with a `frame size exceeded` protocol error, which the handler logs.
    wait_condition(
        lambda: node.contains_in_log("frame size exceeded"),
        lambda fired: fired,
        max_attempts=20,
        delay=0.5,
    )

    # The client must not take the proposed zero at face value: its TuneOk reply has to
    # carry the lower clamp, which is what bounds the receive buffer.
    _assert_negotiated_frame_max("malicious_broker.log", MIN_FRAME_SIZE)


def test_broker_proposing_zero_frame_max_over_tls(started_cluster):
    """The same overflow driven through the TLS (amqps) receive path, which used to be unbounded.

    This is primarily a guard for the sanitizer lane: without the fix the oversized frame is read
    past the end of the TLS receive buffer and a sanitizer build aborts here, so the server must
    stay alive and answer afterwards.
    """
    # The plaintext test already puts `frame size exceeded` into the shared server log, so
    # require the count to grow rather than the substring to appear.
    rejections_before = int(node.count_in_log("frame size exceeded"))

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

    # Deterministic proof the guard fired on the TLS receive path as well, not just that the
    # server survived (on a non-sanitizer build the overflow could go unnoticed otherwise).
    wait_condition(
        lambda: int(node.count_in_log("frame size exceeded")),
        lambda count: count > rejections_before,
        max_attempts=20,
        delay=0.5,
    )

    _assert_negotiated_frame_max("malicious_broker_tls.log", MIN_FRAME_SIZE)


def test_broker_proposing_huge_frame_max(started_cluster):
    """A frame_max proposal close to 4 GiB must be clamped to 128 MiB, not echoed back.

    The TuneOk reply is what bounds the receive buffer, so accepting the proposal verbatim
    would let the broker legally announce frames of arbitrary size.
    """
    rejections_before = int(node.count_in_log("frame size exceeded"))

    error = node.query_and_get_error(
        f"""
        CREATE TABLE malicious_huge (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://guest:guest@localhost:{HUGE_FRAME_MAX_BROKER_PORT}/',
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    assert "CANNOT_CONNECT_RABBITMQ" in error, error
    assert node.query("SELECT 1") == "1\n"

    # The ~4 GiB frame the broker sends next still exceeds the clamped 128 MiB.
    wait_condition(
        lambda: int(node.count_in_log("frame size exceeded")),
        lambda count: count > rejections_before,
        max_attempts=20,
        delay=0.5,
    )

    _assert_negotiated_frame_max("malicious_broker_huge.log", MAX_FRAME_SIZE)


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


def test_rabbitmq_secure_conflicts_with_plaintext_address(started_cluster):
    """`rabbitmq_secure = 1` with a plaintext `amqp://` address must be rejected.

    `RabbitMQConnection::connectImpl` takes the transport from the URI scheme and ignores the
    `rabbitmq_secure` setting for the address form, so this combination used to connect in
    cleartext despite the user having asked for TLS - a silent downgrade rather than an error.
    """
    error = node.query_and_get_error(
        f"""
        CREATE TABLE secure_conflict (key UInt64, value UInt64)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://guest:guest@localhost:{BROKER_PORT}/',
                 rabbitmq_secure = 1,
                 rabbitmq_exchange_name = 'ex',
                 rabbitmq_format = 'JSONEachRow'
        """
    )
    assert "rabbitmq_secure" in error, error
    assert "amqps" in error, error
