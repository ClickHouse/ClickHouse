"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a NATS table."""

# NATS core delivery has no backlog: messages published while there is no subscriber are
# dropped. The STOP/PAUSE tests therefore assert that consumption resumes for freshly
# published messages, not that a backlog is drained.

import asyncio
import json
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster, nats_connect_ssl
from helpers.config_cluster import nats_user, nats_pass

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/nats.xml"],
    with_nats=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    yield


def nats_publish(nats_cluster, subject, start, count):
    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        for i in range(start, start + count):
            await nc.publish(subject, json.dumps({"key": i, "value": i}).encode())
        await nc.flush()
        await nc.close()

    asyncio.run(run())


def jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds):
    """Create a JetStream stream and an explicit-ack durable pull consumer (recreated from scratch)."""
    from nats.js.api import AckPolicy, ConsumerConfig

    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        js = nc.jetstream()
        try:
            await js.delete_stream(stream)
        except Exception:
            pass  # did not exist yet
        await js.add_stream(name=stream, subjects=[subject])
        await js.add_consumer(
            stream,
            ConsumerConfig(
                durable_name=durable,
                filter_subject=subject,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=ack_wait_seconds,
            ),
        )
        await nc.close()

    asyncio.run(run())


def jetstream_publish(nats_cluster, subject, start, count):
    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        js = nc.jetstream()
        for i in range(start, start + count):
            await js.publish(subject, json.dumps({"key": i, "value": i}).encode())
        await nc.close()

    asyncio.run(run())


def jetstream_ack_pending(nats_cluster, stream, durable):
    """Number of messages delivered to the consumer but not yet acknowledged."""

    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        js = nc.jetstream()
        info = await js.consumer_info(stream, durable)
        await nc.close()
        return info.num_ack_pending

    return asyncio.run(run())


def setup_consuming_table(table, subject):
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")


def wait_dst_count_at_least(table, expected):
    instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) >= expected,
        retry_count=120,
        sleep_time=0.5,
    )


def assert_dst_count_stable(table, expected, seconds=5):
    """The consumer is expected to be stopped, so the row count must not grow.
    Still-running consumer polls every kafka_flush_interval_ms (500ms here)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(nats_cluster):
    table = "nats_stop"
    subject = "stop_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # STOP halts consumption: messages published with no subscriber are dropped.
    instance.query(f"SYSTEM STOP test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    # START resumes consumption: freshly published messages are delivered again.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_pause_start_consuming(nats_cluster):
    table = "nats_pause"
    subject = "pause_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    instance.query(f"SYSTEM PAUSE test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_cancel_consuming(nats_cluster):
    table = "nats_cancel"
    subject = "cancel_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # CANCEL interrupts only the current poll; the subscription stays active, so
    # subsequently published messages are still consumed.
    instance.query(f"SYSTEM CANCEL test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_refresh_consuming(nats_cluster):
    table = "nats_refresh"
    subject = "refresh_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # REFRESH kicks off a poll out of order; streaming continues normally.
    instance.query(f"SYSTEM REFRESH test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_refresh_runs_once_while_start_keeps_consuming(nats_cluster):
    # REFRESH runs exactly one streaming cycle out of order without resuming the stream; START
    # resumes continuous streaming. Core NATS has no backlog, so the single REFRESH cycle holds a
    # block open for nats_flush_interval_ms and consumes messages published into that window once;
    # afterwards the stream is stopped again (no active subscription), so later messages are dropped
    # — whereas after START the stream keeps consuming "forever".
    table = "nats_refreshonce"
    subject = "refreshonce_subject"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_flush_interval_ms = 6000,
                     nats_wait_for_flush_interval = 1,
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    # Let the initial streaming round start, then STOP so no cycles run on their own afterwards.
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    instance.query(f"SYSTEM STOP test.{table}")
    time.sleep(5)  # let the in-flight initial round wind down so the table is genuinely stopped

    # REFRESH performs exactly one streaming cycle even while stopped. Give it a moment to start and
    # subscribe, then publish into its open block (held for nats_flush_interval_ms); it consumes once.
    instance.query(f"SYSTEM REFRESH test.{table}")
    time.sleep(2)
    nats_publish(nats_cluster, subject, 0, 5)
    wait_dst_count_at_least(table, 5)

    # The single REFRESH cycle has ended and the stream is still stopped. With no active subscription
    # (core NATS has no backlog) these messages are dropped — REFRESH did not resume the stream.
    time.sleep(3)  # let the one-shot cycle fully end and unsubscribe before publishing again
    nats_publish(nats_cluster, subject, 5, 5)
    assert_dst_count_stable(table, 5, seconds=10)

    # START resumes continuous streaming: subsequently published messages keep being consumed
    # "forever" without any further command.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 10, 5)
    wait_dst_count_at_least(table, 10)


def test_jetstream_acks_after_insert(nats_cluster):
    # Unlike core NATS, JetStream is at-least-once: a message is acknowledged only after it is
    # inserted into the views (its durable boundary). So in steady state every message is consumed
    # exactly once (no redelivery duplicates, the consumer's ack-pending count returns to zero), and
    # messages survive SYSTEM STOP — the stream retains them and redelivers on START.
    stream = "js_stream"
    subject = "js_subject"
    durable = "js_durable"
    table = "nats_jetstream"

    # A short ack-wait so that a missing ack would surface quickly as a redelivery.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=3)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # The messages were acked after insertion: past the 3s ack-wait they are not redelivered, so the
    # count stays at 10 and the consumer reports nothing still pending acknowledgement.
    assert_dst_count_stable(table, 10)
    assert jetstream_ack_pending(nats_cluster, stream, durable) == 0

    # STOP halts consumption; messages published meanwhile are retained by the stream, not lost.
    instance.query(f"SYSTEM STOP test.{table}")
    jetstream_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    # START resumes: the retained messages are delivered and consumed (at-least-once, no loss).
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    wait_dst_count_at_least(table, 20)


def test_stop_aborts_inflight_block_pause_commits_it(nats_cluster):
    # PAUSE lets the in-flight block reach its boundary (the insert into the views) and commit;
    # STOP aborts before it. Core NATS has no broker-side ack or replay, so an aborted block is lost
    # for good — there is nothing to redeliver. `nats_flush_interval_ms` holds the block open long
    # enough for the command to arrive while it is in flight.
    for verb in ["PAUSE", "STOP"]:
        table = f"nats_inflight_{verb.lower()}"
        subject = f"inflight_{verb.lower()}_subject"
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_subjects = '{subject}',
                         nats_format = 'JSONEachRow',
                         nats_flush_interval_ms = 10000,
                         nats_wait_for_flush_interval = 1,
                         nats_secure = 1,
                         nats_username = '{nats_user}',
                         nats_password = '{nats_pass}';

            CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
                ENGINE = MergeTree ORDER BY key;

            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )
        # Wait for this table's streaming round to start so the in-flight block's flush window begins
        # now; the messages published next are read into that block and held (not yet committed).
        instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
        nats_publish(nats_cluster, subject, 0, 5)

        time.sleep(2)  # the block is open with 5 rows, well within the 10s flush window
        instance.query(f"SYSTEM {verb} test.{table}")

        if verb == "PAUSE":
            # The in-flight block is allowed to finish; it commits when the flush window elapses.
            wait_dst_count_at_least(table, 5)
        else:
            # STOP aborts the in-flight block. With core NATS there is no redelivery, so the 5 rows
            # are lost permanently — they never appear, even after START.
            assert_dst_count_stable(table, 0, seconds=12)
            instance.query(f"SYSTEM START test.{table}")
            instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
            assert_dst_count_stable(table, 0)


def test_stop_during_insert_does_not_duplicate(nats_cluster):
    # A STOP arriving while the block is being inserted into the views (the poll already returned it)
    # must let the insert finish and ack its JetStream messages, so they are acked exactly once and
    # never redelivered. The per-row-sleeping view stretches the insert. JetStream (not core NATS) is
    # used so an abort that does land in the poll redelivers rather than loses the messages.
    stream = "js_insert_stream"
    subject = "js_insert_subject"
    durable = "js_insert_durable"
    table = "nats_stop_during_insert"
    n = 5

    # A short ack-wait so a missing ack surfaces quickly as a redelivery.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=3)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, n)

    # Land a STOP while the slow insert is running, then immediately START so that a block whose ack was
    # (incorrectly) skipped would be redelivered after ack_wait and surface as a duplicate.
    time.sleep(1)
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    # The block is acked exactly once: the rows appear and never grow past n (past the 3s ack-wait there
    # is no redelivery), none are missing, and the consumer reports nothing still pending acknowledgement.
    wait_dst_count_at_least(table, n)
    assert_dst_count_stable(table, n, seconds=8)
    assert jetstream_ack_pending(nats_cluster, stream, durable) == 0


def test_cancel_during_insert_does_not_duplicate(nats_cluster):
    # CANCEL companion to test_stop_during_insert_does_not_duplicate (JetStream): a CANCEL during the
    # slow insert must let the block finish and ack exactly once. CANCEL keeps consuming, so a wrongly
    # skipped ack would redeliver after ack_wait on its own and show up as a duplicate (no START needed).
    stream = "js_cancel_insert_stream"
    subject = "js_cancel_insert_subject"
    durable = "js_cancel_insert_durable"
    table = "nats_cancel_during_insert"
    n = 5

    # A short ack-wait so a missing ack surfaces quickly as a redelivery.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=3)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, n)

    # CANCEL during the slow insert; it keeps consuming, so a skipped ack would redeliver after ack_wait.
    time.sleep(1)
    instance.query(f"SYSTEM CANCEL test.{table}")

    # Acked exactly once: count reaches n and never grows (no duplicate, no loss), and nothing is
    # left pending acknowledgement.
    wait_dst_count_at_least(table, n)
    assert_dst_count_stable(table, n, seconds=8)
    assert jetstream_ack_pending(nats_cluster, stream, durable) == 0


def test_cancel_during_direct_select_does_not_drop_messages(nats_cluster):
    # A direct SELECT on a NATS table consumes messages but -- unlike the background view path --
    # performs no acknowledgement of its own (NATSSource never acks/commits). So an aborted direct
    # read has nothing to ack, and a SYSTEM CANCEL racing it cannot commit messages that were never
    # returned. We verify this on JetStream (at-least-once): a message stays redeliverable until it
    # is acked, so a read aborted mid-flight loses nothing. We hammer SYSTEM CANCEL while draining via
    # repeated direct SELECTs, then drain the remainder with no cancels; every published key must
    # surface in some SELECT result.
    stream = "js_cancel_stream"
    subject = "js_cancel_subject"
    durable = "js_cancel_durable"
    table = "nats_direct_cancel"
    n = 10

    # Short ack-wait so messages a direct read pulled but never acked are redelivered quickly.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=2)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';
        """
    )

    jetstream_publish(nats_cluster, subject, 0, n)

    collected = set()

    def drain(deadline):
        while time.time() < deadline and len(collected) < n:
            res = instance.query(
                f"SELECT key FROM test.{table} "
                f"SETTINGS stream_like_engine_allow_direct_select = 1",
                ignore_error=True,
            )
            for line in res.split():
                if line.strip():
                    collected.add(int(line))

    stop = threading.Event()

    def spam_cancel():
        while not stop.is_set():
            instance.query(f"SYSTEM CANCEL test.{table}")
            time.sleep(0.02)

    canceller = threading.Thread(target=spam_cancel)
    canceller.start()
    try:
        drain(time.time() + 15)  # drain under a storm of SYSTEM CANCELs racing the direct reads
    finally:
        stop.set()
        canceller.join()

    drain(time.time() + 120)  # finish draining with no cancels in flight

    missing = set(range(n)) - collected
    assert not missing, (
        f"JetStream messages lost during a cancelled direct read: {sorted(missing)}"
    )


def test_system_stop_all_background(nats_cluster):
    table = "nats_allbg"
    subject = "allbg_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    instance.query("SYSTEM STOP ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    # START ALL BACKGROUND resumes every streaming table.
    instance.query("SYSTEM START ALL BACKGROUND")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_pause_all_background(nats_cluster):
    table = "nats_pauseall"
    subject = "pauseall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # PAUSE ALL BACKGROUND halts consumption for every streaming table.
    instance.query("SYSTEM PAUSE ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query("SYSTEM START ALL BACKGROUND")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_cancel_all_background(nats_cluster):
    table = "nats_cancelall"
    subject = "cancelall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # CANCEL ALL BACKGROUND interrupts the current poll but keeps the subscription, so
    # subsequently published messages are still consumed.
    instance.query("SYSTEM CANCEL ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_refresh_all_background(nats_cluster):
    table = "nats_refreshall"
    subject = "refreshall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # REFRESH ALL BACKGROUND kicks off a poll; streaming continues normally.
    instance.query("SYSTEM REFRESH ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_stop_requires_grant(nats_cluster):
    table = "nats_grant"
    subject = "grant_subject"
    setup_consuming_table(table, subject)
    user = f"user_{table}"
    instance.query(f"DROP USER IF EXISTS {user}; CREATE USER {user}")

    # A user without the required SYSTEM privilege cannot control the table.
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"SYSTEM {verb} test.{table}", user=user
        )

    # SYSTEM VIEWS (the privilege behind the refreshable-view path) is deliberately not enough:
    # streaming engines are guarded by SYSTEM STREAMING ENGINES specifically.
    instance.query(f"GRANT SYSTEM VIEWS ON test.{table} TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"SYSTEM {verb} test.{table}", user=user
        )

    # SYSTEM STREAMING ENGINES on the table is exactly the required privilege; every verb now succeeds.
    instance.query(f"GRANT SYSTEM STREAMING ENGINES ON test.{table} TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        instance.query(f"SYSTEM {verb} test.{table}", user=user)

    instance.query(f"DROP USER {user}")


def test_direct_select_blocked_while_stopped_with_attached_view(nats_cluster):
    # A direct SELECT must be rejected whenever a materialized view is attached, even while STOPped,
    # or it would steal messages meant for the view. The guard reads the attached views live (like
    # StorageKafka2), so it engages the instant the view exists -- no wait for a background tick.
    table = "nats_stopped_direct"
    subject = "stopped_direct_subject"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;
        """
    )
    # Let the consumer connection and consumers initialize, so the direct SELECT below reaches the
    # attached-view guard rather than a "not connected" error.
    time.sleep(3)

    # Stop before any view is attached, then attach the view while stopped.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )

    # The guard is live: a direct SELECT is rejected immediately, with no wait for a background tick.
    def direct_select_error():
        try:
            instance.query(
                f"SELECT count() FROM test.{table}",
                settings={"stream_like_engine_allow_direct_select": 1},
            )
            return ""  # the guard did not engage
        except Exception as e:
            return str(e)

    assert (
        "Cannot read from StorageNATS with attached materialized views"
        in direct_select_error()
    ), "direct SELECT was not blocked immediately after a materialized view was attached"

    # START resumes consumption into the view.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)


def test_refresh_on_stopped_viewless_table_does_not_leak(nats_cluster):
    # SYSTEM REFRESH on a STOPped viewless table must not leave a one-shot cycle armed that later
    # fires when a view is attached, bypassing the STOP. JetStream retains messages, so a leaked
    # refresh would surface them in the view -- we assert nothing is consumed until START.
    stream = "js_leak_stream"
    subject = "js_leak_subject"
    durable = "js_leak_durable"
    table = "nats_refresh_leak"

    # A generous ack-wait so retained messages stay available for the (forbidden) leaked cycle.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=30)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;
        """
    )
    # Let the consumers initialize while the table is viewless.
    time.sleep(3)

    # Messages retained by the stream that a leaked refresh could stream into the view.
    jetstream_publish(nats_cluster, subject, 0, 10)

    # STOP, then REFRESH while still viewless: the one-shot request must be consumed, not left armed.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM REFRESH test.{table}")
    time.sleep(3)  # let the initialize task consume the pending one-shot refresh

    # Attach a view while stopped. A leaked refresh would now run one cycle and consume the backlog.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )

    # STOP holds: no cycle runs, nothing is consumed into the view.
    assert_dst_count_stable(table, 0, seconds=8)

    # START resumes and consumes the retained messages, proving they were available all along.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    wait_dst_count_at_least(table, 10)


def test_refresh_after_detach_reattach_does_not_leak(nats_cluster):
    # A table that HAD a view and then loses it keeps polling via the streaming task (not the
    # initialize task). SYSTEM REFRESH while viewless must still consume the one-shot grant so it
    # cannot fire when a new view is attached and bypass the STOP. JetStream retains messages, so a
    # leaked refresh would surface them in the re-attached view -- we assert nothing until START.
    stream = "js_reattach_stream"
    subject = "js_reattach_subject"
    durable = "js_reattach_durable"
    table = "nats_refresh_reattach"

    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=30)

    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_stream = '{stream}',
                     nats_consumer_name = '{durable}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    # The table starts with a view, so streaming runs via the streaming task.
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    # First batch is consumed and acked normally.
    jetstream_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # Drop the view: the table becomes viewless but keeps polling via the streaming task.
    instance.query(f"DROP TABLE test.{table}_mv SYNC")
    time.sleep(3)  # let the streaming task observe the detach and drop the subscription

    # Messages retained by JetStream that a leaked refresh could later stream into a new view.
    jetstream_publish(nats_cluster, subject, 100, 10)

    # STOP, then REFRESH while viewless: the one-shot request must be consumed, not left armed.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM REFRESH test.{table}")
    time.sleep(3)  # let the streaming task consume the pending one-shot refresh

    # Re-attach a view while stopped. A leaked refresh would now run one cycle into the new view.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )

    # STOP holds: nothing new is consumed into the view.
    assert_dst_count_stable(table, 10, seconds=8)

    # START resumes and consumes the retained messages, proving they were available all along.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    wait_dst_count_at_least(table, 20)


def test_stop_does_not_buffer_backlog(nats_cluster):
    # While STOPped, a core-NATS table must not let messages buffered locally during the stop be
    # delivered after START. We never sleep after STOP: each cycle publishes a distinct range and
    # STARTs immediately, racing the lazy unsubscribe. Several cycles make the race reliable.
    table = "nats_no_backlog"
    subject = "no_backlog_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # STOP -> publish a distinct range -> START with no sleep after STOP, repeated to reliably hit
    # the window where the subscription is still alive while stopped.
    for i in range(8):
        base = 1000 + i * 100
        instance.query(f"SYSTEM STOP test.{table}")
        nats_publish(nats_cluster, subject, base, 20)
        instance.query(f"SYSTEM START test.{table}")

    # Let the stream resume on a fresh subscription before publishing a final, post-stop range.
    time.sleep(3)
    nats_publish(nats_cluster, subject, 9000, 10)
    wait_dst_count_at_least(table, 20)
    time.sleep(3)  # let any incorrectly buffered stopped-interval messages flush before asserting

    leaked = int(
        instance.query(
            f"SELECT count() FROM test.{table}_dst WHERE key >= 1000 AND key < 9000"
        )
    )
    assert (
        leaked == 0
    ), f"{leaked} messages published while stopped were buffered and delivered after START"
