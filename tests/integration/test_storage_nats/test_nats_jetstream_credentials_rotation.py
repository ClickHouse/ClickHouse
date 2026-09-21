"""A JetStream `NATS` table recovers from a connection the client library closed for good.

`test_nats_credentials_rotation.py` proves the recovery for core NATS against a fake broker. The
recovery path is shared with JetStream, whose half of the contract is different: replacing the
connection tears down the durable pull consumer's subscription with messages possibly still
unacknowledged, and the table has to resume through a fresh subscription to the same durable
consumer, with the broker redelivering whatever was not acknowledged. That needs a real broker,
so these tests put an authentication proxy (`nats_auth_proxy.py`) in front of `nats1`, which
rejects the credentials of the live table the way the broker does after a rotation.
"""

import asyncio
import json
import logging
import os.path as p
import time

import pytest

from helpers.cluster import ClickHouseCluster, nats_connect_ssl
from helpers.config_cluster import nats_user, nats_pass

from . import common as nats_helpers

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", with_nats=True)

PROXY_SCRIPT = "nats_auth_proxy.py"
PROXY_IN_CONTAINER = "/" + PROXY_SCRIPT
PROXY_PORT = 4457
PROXY_STATE = "/nats_auth_proxy_state"
PROXY_LOG = "/var/log/clickhouse-server/nats_auth_proxy.log"

# The table reports the closed connection under its own name, with the error the proxy injected.
CONNECTION_CLOSED_LOG_LINE = (
    r"StorageNATS \(test\.nats\): The NATS client library closed the connection to .* "
    r"Last error: Authorization Violation\."
)

STREAM = "rotated_stream"
SUBJECT = "rotated_subject"
DURABLE = "rotated_durable"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        nats_helpers.wait_nats_to_start(cluster)
        start_auth_proxy()
        yield cluster
    finally:
        cluster.shutdown()


def set_proxy_state(state):
    instance.exec_in_container(
        ["bash", "-c", "echo {} > {}".format(state, PROXY_STATE)], user="root"
    )


def proxy_log():
    return instance.exec_in_container(
        ["bash", "-c", "cat {} 2>/dev/null || true".format(PROXY_LOG)], user="root"
    )


def proxy_log_count(needle):
    return proxy_log().count(needle)


def start_auth_proxy():
    set_proxy_state("accept")

    instance.copy_file_to_container(
        p.join(p.dirname(__file__), PROXY_SCRIPT), PROXY_IN_CONTAINER
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 {} {} {} {} 4444 > {} 2>&1".format(
                PROXY_IN_CONTAINER, PROXY_PORT, PROXY_STATE, cluster.nats_host, PROXY_LOG
            ),
        ],
        detach=True,
        user="root",
    )

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if "listening" in proxy_log():
            logging.debug("The NATS authentication proxy is listening")
            return
        time.sleep(0.2)

    raise Exception("The NATS authentication proxy did not start")


def wait_for_proxy_log_count(needle, at_least, time_limit_sec=120):
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if proxy_log_count(needle) >= at_least:
            return
        time.sleep(0.5)

    raise Exception(
        "The proxy log holds {} occurrences of {!r}, expected {}".format(
            proxy_log_count(needle), needle, at_least
        )
    )


def wait_for_connection_closed():
    # The tests reuse the table name, so the wait is anchored past the occurrences the previous
    # tests left in the window `wait_for_log_line` looks at.
    seen = nats_helpers.count_in_recent_log(instance, CONNECTION_CLOSED_LOG_LINE)
    instance.wait_for_log_line(
        CONNECTION_CLOSED_LOG_LINE, timeout=120, repetitions=seen + 1
    )


def jetstream_setup(ack_wait_seconds):
    """A stream and an explicit-ack durable pull consumer, recreated from scratch."""
    from nats.js.api import AckPolicy, ConsumerConfig

    async def run():
        nc = await nats_connect_ssl(cluster)
        js = nc.jetstream()
        try:
            await js.delete_stream(STREAM)
        except Exception:
            pass  # did not exist yet
        await js.add_stream(name=STREAM, subjects=[SUBJECT])
        await js.add_consumer(
            STREAM,
            ConsumerConfig(
                durable_name=DURABLE,
                filter_subject=SUBJECT,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=ack_wait_seconds,
            ),
        )
        await nc.close()

    asyncio.run(run())


def jetstream_publish(start, count):
    async def run():
        nc = await nats_connect_ssl(cluster)
        js = nc.jetstream()
        for i in range(start, start + count):
            await js.publish(SUBJECT, json.dumps({"key": i, "value": i}).encode())
        await nc.close()

    asyncio.run(run())


def jetstream_ack_pending():
    """Number of messages delivered to the durable consumer but not yet acknowledged."""

    async def run():
        nc = await nats_connect_ssl(cluster)
        info = await nc.jetstream().consumer_info(STREAM, DURABLE)
        await nc.close()
        return info.num_ack_pending

    return asyncio.run(run())


def create_pipeline(extra_settings=""):
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '127.0.0.1:{}',
                     nats_subjects = '{}',
                     nats_stream = '{}',
                     nats_consumer_name = '{}',
                     nats_format = 'JSONEachRow',
                     nats_username = '{}',
                     nats_password = '{}',
                     nats_reconnect_wait = 500,
                     nats_startup_connect_tries = 1{};

        CREATE TABLE test.destination (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.destination AS
            SELECT * FROM test.nats;
        """.format(
            PROXY_PORT, SUBJECT, STREAM, DURABLE, nats_user, nats_pass, extra_settings
        )
    )


def consumed():
    return int(instance.query("SELECT count() FROM test.destination"))


def consumed_keys():
    return sorted(
        int(key) for key in instance.query("SELECT DISTINCT key FROM test.destination").split()
    )


def wait_for_consumed_at_least(expected, time_limit_sec=120):
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if consumed() >= expected:
            return
        time.sleep(0.5)

    raise Exception("The table consumed {} messages, expected {}".format(consumed(), expected))


def wait_for_ack_pending(expected, time_limit_sec=60):
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if jetstream_ack_pending() == expected:
            return
        time.sleep(0.5)

    raise Exception(
        "The durable consumer has {} messages pending acknowledgement, expected {}".format(
            jetstream_ack_pending(), expected
        )
    )


def assert_consumed_stays(expected, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        assert consumed() == expected, "The table consumed a message while it was not supposed to"
        time.sleep(1)


def test_jetstream_credentials_rejected_after_rotation(started_cluster):
    set_proxy_state("accept")
    # A short ack-wait: a message which was delivered but not acknowledged when the connection
    # went away is redelivered soon after the table subscribes again.
    jetstream_setup(ack_wait_seconds=3)
    create_pipeline()

    jetstream_publish(0, 10)
    wait_for_consumed_at_least(10)
    wait_for_ack_pending(0)

    # The broker starts rejecting the credentials of the table, the way it does after the password
    # has been rotated on its side, and drops the live connection. The client library retries once
    # and gives up on the second identical authorization error, closing the connection for good.
    set_proxy_state("reject")
    wait_for_connection_closed()
    assert instance.query("SELECT 1") == "1\n"

    # The stream keeps what is published meanwhile: it is the backlog the table has to drain once
    # it is back, and nothing reaches the table while its connection is closed.
    jetstream_publish(10, 10)
    assert_consumed_stays(10)

    # The rotation is rolled back. The table replaces the closed connection and its consumer, and
    # a fresh pull subscription to the same durable consumer drains the backlog: every key exactly
    # once, with nothing left pending acknowledgement.
    set_proxy_state("accept")
    wait_for_consumed_at_least(20)
    wait_for_ack_pending(0)
    assert consumed_keys() == list(range(20))

    # Consumption has resumed for good, not just for the backlog.
    jetstream_publish(20, 10)
    wait_for_consumed_at_least(30)
    wait_for_ack_pending(0)
    assert consumed_keys() == list(range(30))
    assert consumed() == 30, "A message was consumed twice"

    instance.query("DROP DATABASE test SYNC")


def test_jetstream_unacked_messages_survive_rotation(started_cluster):
    """Messages delivered but not acknowledged when the connection closes are redelivered.

    The table acknowledges a message only after it has been inserted into the views. A view whose
    insert fails leaves every delivered message pending, which is the state a table is in when
    the connection dies in the middle of a batch: the old consumer is torn down with the messages
    unacknowledged, and the broker redelivers them to the fresh subscription.
    """
    set_proxy_state("accept")
    jetstream_setup(ack_wait_seconds=3)
    create_pipeline()

    # Swap the working view for one whose insert always fails, so the delivered messages stay
    # pending acknowledgement.
    instance.query("DROP TABLE test.consumer SYNC")
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.destination AS
            SELECT key, throwIf(value < 1000000000, 'insert boom') AS value FROM test.nats;
        """
    )
    jetstream_publish(0, 10)
    wait_for_ack_pending(10)
    assert consumed() == 0

    set_proxy_state("reject")
    wait_for_connection_closed()

    # The messages the old consumer held are still owned by the durable consumer on the broker,
    # not lost with the connection.
    instance.query("DROP TABLE test.consumer SYNC")
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.destination AS
            SELECT * FROM test.nats;
        """
    )
    assert_consumed_stays(0)

    set_proxy_state("accept")
    wait_for_consumed_at_least(10)
    wait_for_ack_pending(0)
    assert consumed_keys() == list(range(10))

    instance.query("DROP DATABASE test SYNC")
