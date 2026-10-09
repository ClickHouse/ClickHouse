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


def count_streaming_starts(table):
    # Occurrences of the "Started streaming" line for this table within the same window
    # wait_for_log_line looks at, so we can wait for a fresh occurrence past the current ones.
    filename = "/var/log/clickhouse-server/clickhouse-server.log"
    result = instance.exec_in_container(
        ["bash", "-c", f"tail -n10000 {filename} | grep -Ec 'test.{table}.*Started streaming to 1 attached views' || true"]
    )
    return int(result.strip() or 0)


def start_and_wait_for_streaming(table, query=None):
    seen = count_streaming_starts(table)
    instance.query(query or f"SYSTEM START test.{table}")
    instance.wait_for_log_line(
        f"test.{table}.*Started streaming to 1 attached views", repetitions=seen + 1
    )


def wait_dst_count_at_least(table, expected):
    result = instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) >= expected,
        retry_count=120,
        sleep_time=0.5,
    )
    assert int(result) >= expected, f"expected at least {expected} rows in {table}_dst, got {result!r}"


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
    start_and_wait_for_streaming(table)
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

    start_and_wait_for_streaming(table)
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
    # - whereas after START the stream keeps consuming "forever".
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
    # (core NATS has no backlog) these messages are dropped - REFRESH did not resume the stream.
    time.sleep(3)  # let the one-shot cycle fully end and unsubscribe before publishing again
    nats_publish(nats_cluster, subject, 5, 5)
    assert_dst_count_stable(table, 5, seconds=10)

    # START resumes continuous streaming: subsequently published messages keep being consumed
    # "forever" without any further command.
    start_and_wait_for_streaming(table)
    nats_publish(nats_cluster, subject, 10, 5)
    wait_dst_count_at_least(table, 10)


def test_jetstream_acks_after_insert(nats_cluster):
    # Unlike core NATS, JetStream is at-least-once: a message is acknowledged only after it is
    # inserted into the views (its durable boundary). So in steady state every message is consumed
    # exactly once (no redelivery duplicates, the consumer's ack-pending count returns to zero), and
    # messages survive SYSTEM STOP - the stream retains them and redelivers on START.
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


def test_jetstream_failed_insert_does_not_lose_messages(nats_cluster):
    # A materialized-view insert that throws must not lose JetStream messages: they are acked only
    # after a successful insert, so while it keeps failing they stay unacked (num_ack_pending == n)
    # and dst stays empty. Swapping in a working view then ingests every key from broker redelivery,
    # proving nothing was dropped (the old auto-ack-on-delivery would have acked and lost them).
    stream = "js_failins_stream"
    subject = "js_failins_subject"
    durable = "js_failins_durable"
    table = "nats_failins"
    n = 10
    # Short ack-wait so the broker keeps redelivering the unacked messages during the test.
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
        """
    )
    # A view whose insert always throws (throwIf fires on every row), so each streaming cycle reads a
    # block, the insert fails, and nothing is acked.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, throwIf(value < 1000000000, 'insert boom') AS value FROM test.{table};
        """
    )
    jetstream_publish(nats_cluster, subject, 0, n)

    # The insert keeps failing: dst stays empty and every delivered message stays pending (unacked), so
    # the broker will redeliver them -- they are not lost.
    deadline = time.time() + 20
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == 0
        if jetstream_ack_pending(nats_cluster, stream, durable) == n:
            break
        time.sleep(0.5)
    assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == 0
    assert jetstream_ack_pending(nats_cluster, stream, durable) == n, (
        "a failing materialized-view insert must not ack JetStream messages"
    )

    # Replace the failing view with a working one; the still-unacked messages are redelivered and
    # ingested, proving the failed inserts did not drop them.
    instance.query(f"DROP TABLE test.{table}_mv SYNC")
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    wait_dst_count_at_least(table, n)
    got = set(
        int(x)
        for x in instance.query(
            f"SELECT DISTINCT key FROM test.{table}_dst ORDER BY key"
        ).split()
        if x.strip()
    )
    assert got == set(range(n)), (
        f"failed insert lost JetStream messages: missing {sorted(set(range(n)) - got)}"
    )


def test_stop_aborts_inflight_block_pause_commits_it(nats_cluster):
    # PAUSE lets the in-flight block reach its boundary (the insert into the views) and commit;
    # STOP aborts before it. Core NATS has no broker-side ack or replay, so an aborted block is lost
    # for good - there is nothing to redeliver. `nats_flush_interval_ms` holds the block open long
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
            # are lost permanently - they never appear, even after START.
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
    # With nats_commit_on_select = 1 a direct read acks only the rows it returns. A read aborted by a racing
    # SYSTEM CANCEL returns and acks nothing, so on JetStream (at-least-once) the message stays redeliverable
    # until a completed read acks it. We hammer SYSTEM CANCEL while draining via repeated direct SELECTs,
    # then drain the rest with no cancels; every published key must surface in some SELECT result.
    stream = "js_cancel_stream"
    subject = "js_cancel_subject"
    durable = "js_cancel_durable"
    table = "nats_direct_cancel"
    n = 10

    # Short ack-wait so messages a read pulled but did not ack (because it was cancelled) redeliver quickly.
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
                     nats_password = '{nats_pass}',
                     nats_commit_on_select = 1;
        """
    )

    jetstream_publish(nats_cluster, subject, 0, n)

    collected = set()

    def drain(deadline):
        while time.time() < deadline and len(collected) < n:
            res = instance.query(
                f"SELECT key FROM test.{table} "
                "SETTINGS stream_like_engine_allow_direct_select = 1",
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


def test_commit_on_select_consumes_only_when_enabled(nats_cluster):
    # A direct read acks the JetStream messages it returns only with nats_commit_on_select = 1 (like
    # kafka_commit_on_select / rabbitmq_commit_on_select). By default (0) nothing is acked, so every message
    # stays pending (num_ack_pending == n); with 1 repeated reads drain and ack them all (pending == 0). The
    # materialized-view path is unaffected (covered by test_jetstream_acks_after_insert).
    n = 5
    for commit_on_select in (0, 1):
        stream = f"js_cos_stream_{commit_on_select}"
        subject = f"js_cos_subject_{commit_on_select}"
        durable = f"js_cos_durable_{commit_on_select}"
        table = f"nats_cos_{commit_on_select}"

        # Short ack-wait so unacked messages are redelivered promptly: a block-size-1 read returns one row
        # per SELECT, so draining every key with commit_on_select = 1 relies on redelivery across ack_wait.
        jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=2)

        # The default (0) is exercised by omitting the setting entirely, proving the default value.
        commit_setting = (
            f",\n                         nats_commit_on_select = {commit_on_select}"
            if commit_on_select
            else ""
        )
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
                         nats_password = '{nats_pass}'{commit_setting};
            """
        )

        jetstream_publish(nats_cluster, subject, 0, n)

        def read_once():
            return instance.query(
                f"SELECT key FROM test.{table} "
                "SETTINGS stream_like_engine_allow_direct_select = 1",
                ignore_error=True,
            )

        def pending():
            return jetstream_ack_pending(nats_cluster, stream, durable)

        if commit_on_select:
            # Each returned row is acked, so across ack_wait redelivery cycles every key gets read and acked
            # and the pending count drains to 0.
            saw_row = False
            deadline = time.time() + 60
            while time.time() < deadline:
                if read_once().strip():
                    saw_row = True
                if saw_row and pending() == 0:
                    break
                time.sleep(0.3)
            assert saw_row, "direct SELECT returned no rows"
            assert pending() == 0, (
                "nats_commit_on_select = 1: a direct SELECT must acknowledge the messages it reads"
            )
        else:
            # Drive reads until the broker has delivered the burst, then keep reading: nothing is ever
            # acked, so the pending count must stay at n.
            deadline = time.time() + 30
            while time.time() < deadline and pending() < n:
                read_once()
            assert pending() == n
            for _ in range(5):
                read_once()
                time.sleep(0.2)
            assert pending() == n, (
                "default nats_commit_on_select = 0: a direct SELECT must not consume messages"
            )


def test_direct_select_leftover_does_not_pollute_view(nats_cluster):
    # A direct SELECT buffers JetStream messages (with their broker handles) in the consumer's local queue
    # and, with commit_on_select=0, never acks them. If a view is attached afterwards, streaming reuses that
    # consumer and must not reprocess those stale copies: acking a handle from the now-closed direct-read
    # subscription crashes the server, and reprocessing duplicates rows once the broker also redelivers.
    stream = "js_pollute_stream"
    subject = "js_pollute_subject"
    durable = "js_pollute_durable"
    table = "nats_pollute"
    n = 10
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

    # One direct read returns a single row (block size 1) and leaves the rest of the delivered burst
    # buffered locally; retry until a read returns, confirming the burst was delivered.
    for _ in range(40):
        res = instance.query(
            f"SELECT key FROM test.{table} SETTINGS stream_like_engine_allow_direct_select = 1",
            ignore_error=True,
        )
        if res.strip():
            break
    assert jetstream_ack_pending(nats_cluster, stream, durable) == n

    # Let the unacked messages pass ack_wait so the broker will redeliver them to the next subscription.
    time.sleep(3)

    # Attach a view: streaming reuses the same consumer, whose local queue still holds the stale copies.
    instance.query(
        f"""
        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    wait_dst_count_at_least(table, n)
    time.sleep(5)  # let any duplicate copies (stale + redelivery) finish landing before checking

    # Every key must appear exactly once: stale local copies must not duplicate the broker's redelivery.
    dups = instance.query(
        f"SELECT key, count() FROM test.{table}_dst GROUP BY key HAVING count() > 1 ORDER BY key"
    )
    assert dups == "", f"stale direct-SELECT copies duplicated rows in the view: {dups}"


def test_commit_on_select_does_not_lose_unreturned_messages(nats_cluster):
    # With nats_commit_on_select = 1 a direct read must ack only the rows it returns; the rest of the
    # delivered burst must stay unacked (num_ack_pending == n - returned) and recoverable. The old
    # JetStream auto-ack acked every delivered message on arrival, so the unreturned remainder was acked
    # at the broker (num_ack_pending == 0) and lost.
    stream = "js_cos_loss_stream"
    subject = "js_cos_loss_subject"
    durable = "js_cos_loss_durable"
    table = "nats_cos_loss"
    n = 10
    # Long ack-wait so redelivery does not change the pending count mid-test: the unreturned-but-unacked
    # messages are observed deterministically via num_ack_pending, not a redelivery race.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=60)
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
                     nats_password = '{nats_pass}',
                     nats_commit_on_select = 1;
        """
    )
    jetstream_publish(nats_cluster, subject, 0, n)

    # One block-size-1 direct read returns a single key; the broker delivers the whole burst into the local
    # buffer regardless. Retry until a read returns, so the assertion does not race the async delivery.
    returned = 0
    deadline = time.time() + 30
    while time.time() < deadline and returned == 0:
        res = instance.query(
            f"SELECT key FROM test.{table} SETTINGS stream_like_engine_allow_direct_select = 1",
            ignore_error=True,
        )
        returned = len([x for x in res.split() if x.strip()])
        if returned == 0:
            time.sleep(0.3)
    assert returned > 0, "direct SELECT returned no rows"

    # Only the returned rows may be acked; the rest of the delivered burst must remain pending (unacked) and
    # thus recoverable. With the auto-ack bug every delivered message is acked, so pending collapses to 0.
    def pending():
        return jetstream_ack_pending(nats_cluster, stream, durable)

    deadline = time.time() + 30
    while time.time() < deadline and pending() != n - returned:
        time.sleep(0.5)
    assert pending() == n - returned, (
        f"with nats_commit_on_select = 1 only the {returned} returned row(s) may be acked; the other "
        f"{n - returned} must stay pending, but pending = {pending()} (0 means the unreturned messages "
        f"were auto-acked and lost)"
    )


def test_commit_on_select_failed_parse_does_not_ack(nats_cluster):
    # A malformed JetStream message read by a direct SELECT with nats_commit_on_select = 1 fails the query
    # while parsing. The message must stay unacknowledged (num_ack_pending == 1) for redelivery, not be
    # acked-and-lost by the source's cleanup as the old unconditional destructor ack did.
    stream = "js_badparse_stream"
    subject = "js_badparse_subject"
    durable = "js_badparse_durable"
    table = "nats_badparse"
    # Long ack-wait so the message is not redelivered mid-test; we observe it stays pending after the failure.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=60)
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
                     nats_password = '{nats_pass}',
                     nats_commit_on_select = 1;
        """
    )

    # Publish one payload that JSONEachRow cannot turn into (key, value), so the read raises while parsing.
    async def publish_bad():
        nc = await nats_connect_ssl(nats_cluster)
        js = nc.jetstream()
        await js.publish(subject, b"not a json row")
        await nc.close()

    asyncio.run(publish_bad())

    # Delivery is async, so retry until a read actually pulls the message and fails; a read that has not
    # received it yet returns no rows and no error.
    failed = False
    deadline = time.time() + 30
    while time.time() < deadline and not failed:
        _, error = instance.query_and_get_answer_with_error(
            f"SELECT key FROM test.{table} SETTINGS stream_like_engine_allow_direct_select = 1"
        )
        if error:
            failed = True
        else:
            time.sleep(0.3)
    assert failed, "a malformed message must make the direct SELECT fail"

    # It was consumed before the failure but must not be acknowledged: it stays pending so JetStream
    # redelivers it. The old destructor ack dropped pending to 0 (the message was lost).
    assert jetstream_ack_pending(nats_cluster, stream, durable) == 1, (
        "a failed direct read must not acknowledge the message it could not parse"
    )


def test_repeated_direct_reads_do_not_return_stale_copies(nats_cluster):
    # A direct read (block size 1) buffers the whole delivered burst locally and never acks it. With a long
    # ack-wait the broker does not redeliver during the test, so after the burst is delivered a later read
    # must return nothing: it drops the buffer before resubscribing. Before the fix the stale copies were
    # handed out again, duplicating the broker's redelivery once ack_wait passed.
    stream = "js_stale_stream"
    subject = "js_stale_subject"
    durable = "js_stale_durable"
    table = "nats_stale"
    n = 10
    # Long ack-wait: the broker will not redeliver within the test, so any row a later read returns can only
    # be a stale local copy.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=60)
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

    # Drive direct reads until the broker has delivered the whole burst (all pending, none acked).
    deadline = time.time() + 30
    while (
        time.time() < deadline
        and jetstream_ack_pending(nats_cluster, stream, durable) < n
    ):
        instance.query(
            f"SELECT key FROM test.{table} SETTINGS stream_like_engine_allow_direct_select = 1",
            ignore_error=True,
        )
    assert jetstream_ack_pending(nats_cluster, stream, durable) == n

    # The burst is delivered and unacked; with a 60s ack-wait the broker does not redeliver now, so further
    # reads must return nothing: the local buffer is dropped on resubscribe and there is nothing fresh.
    returned = 0
    for _ in range(3):
        res = instance.query(
            f"SELECT key FROM test.{table} SETTINGS stream_like_engine_allow_direct_select = 1",
            ignore_error=True,
        )
        returned += len([x for x in res.split() if x.strip()])
    assert returned == 0, f"a direct read returned stale buffered copies ({returned} rows)"


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
    start_and_wait_for_streaming(table)
    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)


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


def test_stop_while_viewless_does_not_drop_after_start(nats_cluster):
    # A STOP/PAUSE issued while the table is viewless (unsubscribed) must not leave a stale-unsubscribe
    # armed that fires after START and drops core-NATS messages once the table is streaming again.
    table = "nats_stale_viewless"
    subject = "stale_viewless_subject"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_flush_interval_ms = 500,
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
        """
    )
    time.sleep(3)  # let consumers initialize while viewless

    # STOP while viewless arms the stale-unsubscribe; the bug lets it survive past START.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM START test.{table}")

    # Attach a view: the table subscribes and starts streaming.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")
    time.sleep(3)  # let a surviving stale flag fire its spurious unsubscribe + resubscribe

    # Streaming still works once the dust settles.
    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # The consumer must have subscribed exactly once. A STOP-while-viewless that left the stale flag
    # armed forces a spurious unsubscribe + resubscribe after START -- a second subscribe for the subject.
    subscribes = int(instance.count_in_log(f"Subscribed to subject {subject}"))
    assert (
        subscribes == 1
    ), f"consumer subscribed {subscribes}x; a stale unsubscribe fired after a STOP-while-viewless"


def _server_cpu_jiffies():
    """utime + stime of the clickhouse server process, in clock ticks."""
    pid = instance.get_process_pid("clickhouse server")
    content = instance.exec_in_container(["bash", "-c", f"cat /proc/{pid}/stat"])
    # Skip 'pid (comm)' -- comm may contain spaces -- then fields start at 'state' (field 3).
    rest = content[content.rindex(")") + 1:].split()
    return int(rest[11]) + int(rest[12])  # utime (field 14) + stime (field 15)


def _cpu_over(seconds):
    before = _server_cpu_jiffies()
    time.sleep(seconds)
    return _server_cpu_jiffies() - before


def test_detach_last_view_does_not_busy_loop(nats_cluster):
    # After the last view is detached, the viewless streaming task must back off, not tight-loop the
    # message-broker schedule pool. Compare server CPU with a view (idle 500ms polling) vs viewless;
    # a busy-loop pegs roughly a full core, while backing off stays near the baseline.
    table = "nats_detach_loop"
    subject = "detach_loop_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 5)
    wait_dst_count_at_least(table, 5)

    baseline = _cpu_over(4)

    instance.query(f"DROP TABLE test.{table}_mv SYNC")
    time.sleep(2)  # settle into the viewless state

    viewless = _cpu_over(4)
    assert viewless < baseline + 150, ( # based on test runs, where it's ~15, or ~400-800 for busy
        f"viewless streaming task appears to busy-loop: baseline={baseline} viewless={viewless} "
        "CPU jiffies over 4s"
    )


def test_rapid_stop_start_cycles_drain_unsubscribe(nats_cluster):
    # Hammer STOP/START so INATSConsumer::unsubscribe()'s always-drain + resubscribe path runs many
    # times. The drain fences the onMsg callback before destroy; JetStream is at-least-once, so every
    # message must still arrive and the server must stay up (the use-after-free is caught by TSan CI).
    stream = "js_cycle_stream"
    subject = "js_cycle_subject"
    durable = "js_cycle_durable"
    table = "nats_stop_start_cycles"

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

    total = 0
    for _ in range(8):
        jetstream_publish(nats_cluster, subject, total, 5)
        total += 5
        instance.query(f"SYSTEM STOP test.{table}")  # drains + unsubscribes
        jetstream_publish(nats_cluster, subject, total, 5)  # retained by the stream while stopped
        total += 5
        instance.query(f"SYSTEM START test.{table}")  # resubscribes

    # JetStream redelivers everything published while stopped, so every message eventually arrives.
    wait_dst_count_at_least(table, total)
    assert instance.query("SELECT 1") == "1\n"


def jetstream_delete_stream(nats_cluster, stream):
    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        js = nc.jetstream()
        try:
            await js.delete_stream(stream)
        except Exception:
            pass  # already gone
        await nc.close()

    asyncio.run(run())


def test_resubscribe_failure_backs_off(nats_cluster):
    # A transient subscribe failure on the STOP/PAUSE -> START resubscribe path must back off
    # (reschedule with a delay), not busy-spin the message-broker pool. Inject the failure by deleting
    # the JetStream stream while stopped: the TCP connection stays up (isConnected() is true) but the
    # resubscribe throws. After START the worker must retry at the ~RESCHEDULE_MS backoff rate.
    stream = "js_resub_fail_stream"
    subject = "js_resub_fail_subject"
    durable = "js_resub_fail_durable"
    table = "nats_resub_fail"

    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=5)
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

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, 2)
    wait_dst_count_at_least(table, 2)

    instance.query(f"SYSTEM STOP test.{table}")
    time.sleep(1)  # let the worker unsubscribe and settle

    # Inject: delete the stream so the resubscribe throws while the connection stays up.
    jetstream_delete_stream(nats_cluster, stream)

    marker = f"test.{table}.*Failed to subscribe consumer"
    before = int(instance.count_in_log(marker))
    instance.query(f"SYSTEM START test.{table}")

    window = 5
    time.sleep(window)
    attempts = int(instance.count_in_log(marker)) - before

    # The failure must actually be injected, else the test proves nothing.
    assert attempts > 0, "resubscribe failure was not injected (stream delete had no effect)"
    # Backoff is ~one attempt per RESCHEDULE_MS (500ms) => ~10 over 5s; a busy-loop would be far more.
    assert attempts < 25, (
        f"resubscribe appears to busy-spin: {attempts} subscribe attempts in {window}s "
        "(expected ~10 with 500ms backoff)"
    )

    # Server stays alive throughout.
    assert instance.query("SELECT 1") == "1\n"


def test_refresh_survives_failed_resubscribe(nats_cluster):
    # A REFRESH claimed by a round whose resubscribe fails (stream deleted, connection up) must be
    # retried once subscribing succeeds again, not lost. Regression: the round consumed the one-shot
    # permit and then bailed on the failed subscribe before any streaming.
    stream = "js_refresh_resub_stream"
    subject = "js_refresh_resub_subject"
    durable = "js_refresh_resub_durable"
    table = "nats_refresh_resub"

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
                     nats_password = '{nats_pass}',
                     nats_flush_interval_ms = 6000,
                     nats_wait_for_flush_interval = 1;

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, 2)
    wait_dst_count_at_least(table, 2)

    instance.query(f"SYSTEM STOP test.{table}")
    time.sleep(1)  # let the worker unsubscribe and settle

    # Make the resubscribe fail while the connection stays up, then refresh into that failure.
    jetstream_delete_stream(nats_cluster, stream)
    marker = f"test.{table}.*Failed to subscribe consumer"
    before = int(instance.count_in_log(marker))
    instance.query(f"SYSTEM REFRESH test.{table}")
    instance.wait_for_log_line(marker, repetitions=before + 1)

    # Heal: recreate the stream and publish a backlog; the pending refresh must consume it.
    # Its cycle holds the block open for the flush interval, so it sees a late publish too.
    jetstream_setup(nats_cluster, stream, subject, durable, ack_wait_seconds=30)
    jetstream_publish(nats_cluster, subject, 2, 5)
    wait_dst_count_at_least(table, 7)
    assert_dst_count_stable(table, 7, seconds=5)


def test_refresh_survives_unready_dependencies(nats_cluster):
    # A REFRESH issued while the dependent view is attached but not ready (its target table
    # detached) must run once the target returns, not be lost: a streaming round consumed the
    # one-shot permit and then bailed on the dependency check before any streaming.
    stream = "js_refresh_unready_stream"
    subject = "js_refresh_unready_subject"
    durable = "js_refresh_unready_durable"
    table = "nats_refresh_unready"

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
                     nats_password = '{nats_pass}',
                     nats_flush_interval_ms = 6000,
                     nats_wait_for_flush_interval = 1;

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line(f"test.{table}.*Started streaming to 1 attached views")

    jetstream_publish(nats_cluster, subject, 0, 1)
    wait_dst_count_at_least(table, 1)

    instance.query(f"SYSTEM STOP test.{table}")
    time.sleep(1)  # let the worker unsubscribe and settle
    jetstream_publish(nats_cluster, subject, 1, 5)
    assert_dst_count_stable(table, 1)

    instance.query(f"DETACH TABLE test.{table}_dst")
    instance.query(f"SYSTEM REFRESH test.{table}")
    time.sleep(2)  # streaming rounds wake with the view unready and must keep the permit

    # Once the target table is back, the queued REFRESH must still drain the stream backlog.
    instance.query(f"ATTACH TABLE test.{table}_dst")
    wait_dst_count_at_least(table, 6)
    assert_dst_count_stable(table, 6, seconds=5)
