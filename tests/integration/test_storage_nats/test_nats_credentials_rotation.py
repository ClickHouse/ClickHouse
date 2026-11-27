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
        start_fake_broker()
        yield cluster
    finally:
        cluster.shutdown()


def set_broker_state(state):
    instance.exec_in_container(
        ["bash", "-c", "echo {} > {}".format(state, BROKER_STATE)], user="root"
    )


def broker_log():
    return instance.exec_in_container(
        ["bash", "-c", "cat {} 2>/dev/null || true".format(BROKER_LOG)], user="root"
    )


def broker_log_count(needle):
    return broker_log().count(needle)


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
        if "listening" in broker_log():
            logging.debug("The fake NATS broker is listening")
            return
        time.sleep(0.2)

    raise Exception("The fake NATS broker did not start")


def create_pipeline(subject, extra_settings=""):
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '127.0.0.1:{}',
                     nats_subjects = '{}',
                     nats_format = 'JSONEachRow',
                     nats_username = 'clickhouse',
                     nats_password = 'the_original_one',
                     nats_reconnect_wait = 500,
                     nats_startup_connect_tries = 1{};

        CREATE TABLE test.destination (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.destination AS
            SELECT * FROM test.nats;
        """.format(
            BROKER_PORT, subject, extra_settings
        )
    )


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

    A streaming cycle which was in flight when the table stopped consuming still inserts what it
    had, so the count is only a usable baseline once it has settled.
    """
    previous = None
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        current = consumed()
        if current == previous:
            return current
        previous = current
        time.sleep(3)

    raise Exception("The table kept consuming after it was supposed to stop")


def wait_for_broker_log_count(needle, at_least, time_limit_sec=120):
    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if broker_log_count(needle) >= at_least:
            return
        time.sleep(0.5)

    raise Exception(
        "The broker log holds {} occurrences of {!r}, expected {}".format(
            broker_log_count(needle), needle, at_least
        )
    )


def test_nats_credentials_rejected_after_rotation(started_cluster):
    set_broker_state("accept")
    create_pipeline("rotated_subject")

    # The pipeline is live: the broker delivers a message to every subscription it holds.
    wait_for_consumed_above(0)

    # The broker starts rejecting the credentials of the table, the way it does after the password
    # has been rotated on its side, and drops the live connection. The client library retries once
    # and gives up on the second identical authorization error, closing the connection for good.
    # The table reports the closed connection under its own name, with the error the broker
    # answered, so that an operator with several NATS tables can tell whose credentials to fix.
    set_broker_state("reject")
    instance.wait_for_log_line(
        r"StorageNATS \(test\.nats\): The NATS client library closed the connection to .* "
        r"Last error: Authorization Violation\.",
        timeout=120,
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


def test_stopped_nats_table_does_not_resubscribe_after_rotation(started_cluster):
    """A table recovering from a closed connection must still honour `SYSTEM STOP`.

    Replacing the connection also resubscribes, and a stopped table must hold no subscription:
    with core NATS a message delivered to it is dropped, and in a queue group it is taken away
    from the members which are still running.
    """
    set_broker_state("accept")
    create_pipeline("stopped_subject")
    wait_for_consumed_above(0)

    instance.query("SYSTEM STOP test.nats")
    consumed_while_stopped = wait_for_consumption_to_stop()

    # Reject the credentials until the client library gives up on the connection, then accept
    # them again, which is when a table which is not stopped rebuilds its connection.
    rejections_before = broker_log_count("rejecting the credentials")
    set_broker_state("reject")
    wait_for_broker_log_count("rejecting the credentials", rejections_before + 2)

    subscriptions_before = broker_log_count("subscribed sid")
    set_broker_state("accept")

    time.sleep(15)
    assert (
        broker_log_count("subscribed sid") == subscriptions_before
    ), "A stopped table subscribed while recovering from a closed connection"
    assert consumed() == consumed_while_stopped, "A stopped table consumed a message"
    assert instance.query("SELECT 1") == "1\n"

    # The table is released, and only now is it allowed to rebuild the connection and resume.
    instance.query("SYSTEM START test.nats")
    wait_for_consumed_above(consumed_while_stopped)

    instance.query("DROP DATABASE test SYNC")


def test_stopped_nats_table_refreshes_after_rotation(started_cluster):
    """`SYSTEM REFRESH` on a stopped table still runs its one-shot cycle after a closed connection.

    A stopped table holds no subscription but does keep its connection, so once the client library
    has closed that connection the table has to rebuild it - without subscribing - for the one
    out-of-order cycle a `SYSTEM REFRESH` entitles it to, and for `SYSTEM START` to find it ready.
    """
    set_broker_state("accept")
    # The one-shot cycle of a refresh ends as soon as the consumer has nothing buffered, which is
    # right away when it has just subscribed. Hold its block open for a while instead, so that
    # the broker gets to deliver into it.
    create_pipeline(
        "refreshed_subject",
        ", nats_flush_interval_ms = 3000, nats_wait_for_flush_interval = 1",
    )
    wait_for_consumed_above(0)

    instance.query("SYSTEM STOP test.nats")
    consumed_while_stopped = wait_for_consumption_to_stop()

    rejections_before = broker_log_count("rejecting the credentials")
    set_broker_state("reject")
    wait_for_broker_log_count("rejecting the credentials", rejections_before + 2)

    # The credentials are accepted again: the stopped table rebuilds its connection, and nothing
    # else - it does not subscribe and consumes nothing.
    accepted_before = broker_log_count("credentials accepted")
    subscriptions_before = broker_log_count("subscribed sid")
    set_broker_state("accept")
    wait_for_broker_log_count("credentials accepted", accepted_before + 1)
    assert (
        broker_log_count("subscribed sid") == subscriptions_before
    ), "A stopped table subscribed while rebuilding a closed connection"
    assert consumed() == consumed_while_stopped, "A stopped table consumed a message"

    # The one-shot cycle of a refresh: subscribe, consume what the broker delivers, unsubscribe.
    instance.query("SYSTEM REFRESH test.nats")
    wait_for_consumed_above(consumed_while_stopped)
    consumed_after_refresh = wait_for_consumption_to_stop()

    # The refresh did not resume the stream: the table is still stopped, holds no subscription
    # and consumes nothing.
    subscriptions_after_refresh = broker_log_count("subscribed sid")
    time.sleep(10)
    assert (
        broker_log_count("subscribed sid") == subscriptions_after_refresh
    ), "A refreshed table stayed subscribed after its one-shot cycle"
    assert consumed() == consumed_after_refresh, "A refreshed table kept consuming"

    instance.query("SYSTEM START test.nats")
    wait_for_consumed_above(consumed_after_refresh)

    instance.query("DROP DATABASE test SYNC")
