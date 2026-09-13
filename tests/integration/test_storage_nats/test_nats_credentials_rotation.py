import logging
import os.path as p
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")

BROKER_SCRIPT = "nats_fake_broker.py"
BROKER_IN_CONTAINER = "/" + BROKER_SCRIPT
BROKER_PORT = 4456
BROKER_STATE = "/nats_fake_broker_state"
BROKER_LOG = "/var/log/clickhouse-server/nats_fake_broker.log"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_broker_state(state):
    instance.exec_in_container(
        ["bash", "-c", "echo {} > {}".format(state, BROKER_STATE)], user="root"
    )


def start_fake_broker():
    set_broker_state("accept")

    instance.copy_file_to_container(
        p.join(p.dirname(__file__), BROKER_SCRIPT), BROKER_IN_CONTAINER
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 {} {} {} > {} 2>&1".format(
                BROKER_IN_CONTAINER, BROKER_PORT, BROKER_STATE, BROKER_LOG
            ),
        ],
        detach=True,
        user="root",
    )

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        broker_log = instance.exec_in_container(
            ["bash", "-c", "cat {} 2>/dev/null || true".format(BROKER_LOG)],
            user="root",
        )
        if "listening" in broker_log:
            logging.debug("The fake NATS broker is listening")
            return
        time.sleep(0.2)

    raise Exception("The fake NATS broker did not start")


def consumed():
    return int(instance.query("SELECT count() FROM test.destination"))


def wait_for_consumed_above(consumed_before, time_limit_sec=120):
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if consumed() > consumed_before:
            return
        time.sleep(0.5)

    raise Exception(
        "The table consumed no message beyond the {} it had".format(consumed_before)
    )


def wait_for_consumption_to_stop(time_limit_sec=60):
    """Returns the number of consumed messages once it has stopped growing.

    A streaming cycle which was in flight when the broker dropped the connection still inserts
    what it had, so the count is only a usable baseline once it has settled.
    """
    previous = None
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        current = consumed()
        if current == previous:
            return current
        previous = current
        time.sleep(3)

    raise Exception("The table kept consuming while the broker was rejecting its credentials")


def test_nats_credentials_rejected_after_rotation(started_cluster):
    start_fake_broker()

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '127.0.0.1:{}',
                     nats_subjects = 'rotated_subject',
                     nats_format = 'JSONEachRow',
                     nats_username = 'clickhouse',
                     nats_password = 'the_original_one',
                     nats_reconnect_wait = 500,
                     nats_startup_connect_tries = 1;

        CREATE TABLE test.destination (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.destination AS
            SELECT * FROM test.nats;
        """.format(
            BROKER_PORT
        )
    )

    # The pipeline is live: the broker delivers a message to every subscription it holds.
    wait_for_consumed_above(0)

    # The broker starts rejecting the credentials of the table, the way it does after the password
    # has been rotated on its side, and drops the live connection. The client library retries once
    # and gives up on the second identical authorization error, closing the connection for good.
    set_broker_state("reject")
    instance.wait_for_log_line(
        "The NATS client library closed the connection to", timeout=120
    )

    # The connection was closed on the thread which serves every NATS table of the server, and
    # before the fix the event loop adapter segfaulted there, taking the whole server down.
    assert instance.query("SELECT 1") == "1\n"

    consumed_before = wait_for_consumption_to_stop()

    # The rotation is rolled back. Nothing in the client library reopens a closed connection, so
    # the table has to notice and build a new one, otherwise it would stay idle until it is
    # detached and attached again.
    set_broker_state("accept")
    wait_for_consumed_above(consumed_before)

    instance.query("DROP DATABASE test SYNC")
