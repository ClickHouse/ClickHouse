"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a RabbitMQ table."""

import json
import threading
import time

import pika
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/rabbitmq.xml", "configs/macros.xml"],
    user_configs=["configs/users.xml"],
    with_rabbitmq=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    yield


def publish(rabbitmq_cluster, exchange, start, count):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.confirm_delivery()
    for i in range(start, start + count):
        channel.basic_publish(
            exchange=exchange, routing_key="", body=json.dumps({"key": i, "value": i})
        )
    connection.close()


def publish_raw(rabbitmq_cluster, exchange, body):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.confirm_delivery()
    channel.basic_publish(exchange=exchange, routing_key="", body=body)
    connection.close()


def setup_consuming_table(table, exchange):
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")


def wait_dst_count(table, expected):
    result = instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )
    assert int(result) == expected, f"expected {expected} rows in {table}_dst, got {result!r}"


def assert_dst_count_stable(table, expected, seconds=5):
    """The consumer is expected to be stopped, so the row count must not grow.
    Still-running consumer polls every kafka_flush_interval_ms (500ms here)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(rabbitmq_cluster):
    table = "rabbitmq_stop"
    exchange = "stop_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # STOP halts consumption; messages published meanwhile stay queued in RabbitMQ.
    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 10, 10)
    assert_dst_count_stable(table, 10)

    # START resumes consumption and drains the backlog.
    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 20)


def test_system_pause_start_consuming(rabbitmq_cluster):
    table = "rabbitmq_pause"
    exchange = "pause_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # PAUSE stops further activity: subsequent messages stay queued, unread.
    instance.query(f"SYSTEM PAUSE test.{table}")
    publish(rabbitmq_cluster, exchange, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 20)


def test_system_cancel_consuming(rabbitmq_cluster):
    table = "rabbitmq_cancel"
    exchange = "cancel_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # CANCEL interrupts only the current poll; consumption keeps going afterwards.
    instance.query(f"SYSTEM CANCEL test.{table}")
    publish(rabbitmq_cluster, exchange, 10, 10)
    wait_dst_count(table, 20)


def test_system_refresh_consuming(rabbitmq_cluster):
    table = "rabbitmq_refresh"
    exchange = "refresh_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # REFRESH kicks off a poll out of order; streaming continues normally.
    instance.query(f"SYSTEM REFRESH test.{table}")
    publish(rabbitmq_cluster, exchange, 10, 10)
    wait_dst_count(table, 20)


def test_refresh_runs_once_while_start_keeps_consuming(rabbitmq_cluster):
    # REFRESH runs exactly one consumption cycle out of order without resuming the stream; START
    # resumes continuous consumption. With the stream STOPped, a single REFRESH drains exactly the
    # messages queued in the broker at that moment, and messages published afterwards stay queued
    # until START — whereas after START every later batch is consumed without any further command.
    table = "rabbitmq_refreshonce"
    exchange = "refreshonce_exchange"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    publish(rabbitmq_cluster, exchange, 0, 1)
    wait_dst_count(table, 1)

    instance.query(f"SYSTEM STOP test.{table}")
    time.sleep(2)  # let the in-flight cycle finish so the table is genuinely stopped

    # First batch (queued in the broker while stopped), then one REFRESH: the single cycle drains
    # exactly these messages.
    publish(rabbitmq_cluster, exchange, 1, 5)
    instance.query(f"SYSTEM REFRESH test.{table}")
    wait_dst_count(table, 6)

    # Second batch, no further REFRESH: the stream is still stopped, so it stays queued, unread.
    publish(rabbitmq_cluster, exchange, 6, 5)
    assert_dst_count_stable(table, 6)

    # START resumes continuous consumption.
    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 11)
    publish(rabbitmq_cluster, exchange, 11, 5)
    wait_dst_count(table, 16)


def test_stop_aborts_inflight_block_pause_commits_it(rabbitmq_cluster):
    # The durable boundary for RabbitMQ is the AMQP ack. PAUSE lets the in-flight block reach it
    # (the messages are acked and committed); STOP aborts before it (the messages are requeued and
    # redelivered on resume). A long flush interval keeps the block open while the command arrives.
    for verb in ["PAUSE", "STOP"]:
        table = f"rabbitmq_inflight_{verb.lower()}"
        exchange = f"inflight_{verb.lower()}_exchange"
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = '{exchange}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_queue_base = '{exchange}',
                         rabbitmq_flush_interval_ms = 5000,
                         rabbitmq_max_block_size = 1000,
                         rabbitmq_row_delimiter = '\\n';

            CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
                ENGINE = MergeTree ORDER BY key;

            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )
        instance.wait_for_log_line("Started streaming to 1 attached views")

        # Pre-load while halted, then resume so a fresh cycle opens a block over the 5 rows and holds
        # it for ~rabbitmq_flush_interval_ms (it does not ack/commit until then).
        instance.query(f"SYSTEM STOP test.{table}")
        publish(rabbitmq_cluster, exchange, 0, 5)
        instance.query(f"SYSTEM START test.{table}")

        time.sleep(2)  # the block is now open: messages polled but not yet acked
        instance.query(f"SYSTEM {verb} test.{table}")

        if verb == "PAUSE":
            # The in-flight block finishes and is acked on its own, without START.
            wait_dst_count(table, 5)
        else:
            # The in-flight block is aborted and the messages are requeued (never acked), so they
            # stay invisible until START redelivers them.
            assert_dst_count_stable(table, 0, seconds=8)
            instance.query(f"SYSTEM START test.{table}")
            wait_dst_count(table, 5)


def test_stop_during_insert_does_not_duplicate(rabbitmq_cluster):
    # A STOP arriving while the block is being inserted into the views (the poll already returned it)
    # must let the insert finish and ack, so the messages are acked exactly once and never requeued.
    # The per-row-sleeping view stretches the insert; max_block_size = n makes the poll return at once,
    # so the block is already past the source when STOP arrives.
    table = "rabbitmq_stop_during_insert"
    exchange = "stop_during_insert_exchange"
    n = 5
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_max_block_size = {n},
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    # Halt, pre-load exactly one block, then resume: the source polls all n rows at once
    # (max_block_size = n, no flush wait) and hands them to the ~n*0.4s insert.
    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 0, n)
    instance.query(f"SYSTEM START test.{table}")

    # Land a STOP while the slow insert is running, then immediately START so that a block which was
    # (incorrectly) requeued instead of acked would be redelivered and surface as a duplicate.
    time.sleep(1)
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM START test.{table}")

    # The block reaches the ack exactly once: the rows appear and never grow past n (no duplicate),
    # and none are missing (no loss).
    wait_dst_count(table, n)
    assert_dst_count_stable(table, n, seconds=8)


def test_cancel_during_insert_does_not_duplicate(rabbitmq_cluster):
    # CANCEL companion to test_stop_during_insert_does_not_duplicate: a CANCEL during the slow insert
    # must let the block finish and ack exactly once. CANCEL keeps consuming, so a wrongly requeued
    # block would be redelivered on its own and show up as a duplicate (no START needed).
    table = "rabbitmq_cancel_during_insert"
    exchange = "cancel_during_insert_exchange"
    n = 5
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_max_block_size = {n},
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    # Halt, pre-load exactly one block, then resume: the source polls all n rows at once
    # (max_block_size = n, no flush wait) and hands them to the ~n*0.4s insert.
    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 0, n)
    instance.query(f"SYSTEM START test.{table}")

    # CANCEL during the slow insert; it keeps consuming, so a requeued block would redeliver.
    time.sleep(1)
    instance.query(f"SYSTEM CANCEL test.{table}")

    # Acked exactly once: count reaches n and never grows (no duplicate, no loss).
    wait_dst_count(table, n)
    assert_dst_count_stable(table, n, seconds=8)


def test_system_stop_all_background(rabbitmq_cluster):
    table = "rabbitmq_allbg"
    exchange = "allbg_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    instance.query("SYSTEM STOP ALL BACKGROUND")
    publish(rabbitmq_cluster, exchange, 10, 10)
    assert_dst_count_stable(table, 10)

    # START ALL BACKGROUND resumes every streaming table.
    instance.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(table, 20)


def test_system_pause_all_background(rabbitmq_cluster):
    table = "rabbitmq_pauseall"
    exchange = "pauseall_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # PAUSE ALL BACKGROUND halts consumption for every streaming table.
    instance.query("SYSTEM PAUSE ALL BACKGROUND")
    publish(rabbitmq_cluster, exchange, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(table, 20)


def test_system_cancel_all_background(rabbitmq_cluster):
    table = "rabbitmq_cancelall"
    exchange = "cancelall_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # CANCEL ALL BACKGROUND interrupts the current poll but does not halt the stream.
    instance.query("SYSTEM CANCEL ALL BACKGROUND")
    publish(rabbitmq_cluster, exchange, 10, 10)
    wait_dst_count(table, 20)


def test_system_refresh_all_background(rabbitmq_cluster):
    table = "rabbitmq_refreshall"
    exchange = "refreshall_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # REFRESH ALL BACKGROUND kicks off a poll for every streaming table; the stream continues.
    instance.query("SYSTEM REFRESH ALL BACKGROUND")
    publish(rabbitmq_cluster, exchange, 10, 10)
    wait_dst_count(table, 20)


def test_cancel_during_direct_select_does_not_drop_messages(rabbitmq_cluster):
    # With rabbitmq_commit_on_select = 1 a direct SELECT acks the rows it returns to the client.
    # A SYSTEM CANCEL that aborts an in-flight direct read must not ack that aborted block: its rows
    # are discarded rather than returned, so acking them would silently drop them from the queue.
    # We hammer SYSTEM CANCEL while draining with repeated direct SELECTs, then drain the remainder
    # with no cancels in flight; every published key must still surface in some SELECT result.
    table = "rabbitmq_direct_cancel"
    exchange = "direct_cancel_exchange"
    n = 20
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_row_delimiter = '\\n';
        """
    )
    # Let the engine declare and bind its queue so messages published below are retained for the
    # on-demand direct-read consumer (no materialized view is attached, so nothing consumes in the
    # background).
    time.sleep(3)
    publish(rabbitmq_cluster, exchange, 0, n)

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
        drain(time.time() + 20)  # drain under a storm of SYSTEM CANCELs racing the direct reads
    finally:
        stop.set()
        canceller.join()

    drain(time.time() + 30)  # finish draining with no cancels in flight

    missing = set(range(n)) - collected
    assert not missing, (
        f"messages acked by an aborted direct read but never returned (lost): {sorted(missing)}"
    )


def test_system_stop_requires_grant(rabbitmq_cluster):
    table = "rabbitmq_grant"
    exchange = "grant_exchange"
    setup_consuming_table(table, exchange)
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


def test_direct_select_blocked_while_stopped_with_attached_view(rabbitmq_cluster):
    # While a table with an attached materialized view is STOPped, a direct SELECT must still be
    # rejected: the direct-read guard depends on the attached view, not on whether streaming is
    # running. Otherwise the SELECT would consume messages meant for the view and lose them.
    table = "rabbitmq_stopped_direct"
    exchange = "stopped_direct_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)

    # STOP halts consumption; the 10 new messages stay queued in RabbitMQ.
    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 10, 10)
    # Wait out several stopped polling cycles: if the guard were wrongly dropped while stopped,
    # it would reliably be dropped by the direct read below (so this is a true fail-first check).
    assert_dst_count_stable(table, 10)

    # The view is still attached, so the direct SELECT must be rejected (not steal the backlog).
    err = instance.query_and_get_error(
        f"SELECT count() FROM test.{table}",
        settings={"stream_like_engine_allow_direct_select": 1},
        timeout=60,
    )
    assert "Cannot read from StorageRabbitMQ with attached materialized views" in err

    # START drains the backlog, proving the messages were retained.
    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 20)


def test_direct_select_rejected_when_view_attached_while_stopped(rabbitmq_cluster):
    # The direct-read guard reads the attached views live, so attaching a view to an already STOPped
    # table rejects a direct SELECT immediately, with no wait for a background tick.
    table = "rabbitmq_stopped_attach"
    exchange = "stopped_attach_exchange"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 500,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;
        """
    )

    # read() returns an empty source until consumers are created, so wait for setup to finish first;
    # otherwise the direct SELECT below would not yet reach the attached-view guard.
    def initialized():
        try:
            instance.query(
                f"SELECT count() FROM test.{table}",
                settings={"stream_like_engine_allow_direct_select": 1},
            )
            return True
        except Exception:
            return False

    for _ in range(60):
        if initialized():
            break
        time.sleep(0.5)
    else:
        assert False, "RabbitMQ table did not finish setup"

    # Stop before any view is attached, then attach the view while stopped.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(
        f"CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst "
        f"AS SELECT key, value FROM test.{table}"
    )

    # The guard is live: the direct SELECT is rejected immediately.
    err = instance.query_and_get_error(
        f"SELECT count() FROM test.{table}",
        settings={"stream_like_engine_allow_direct_select": 1},
        timeout=60,
    )
    assert "Cannot read from StorageRabbitMQ with attached materialized views" in err

    # START drains freshly published messages into the view.
    instance.query(f"SYSTEM START test.{table}")
    publish(rabbitmq_cluster, exchange, 0, 10)
    wait_dst_count(table, 10)


def test_cancel_wakes_flush_wait_promptly(rabbitmq_cluster):
    # With a long rabbitmq_flush_interval_ms, a source that has read some rows parks in
    # RabbitMQConsumer::waitForMessages until the flush timeout. SYSTEM CANCEL must wake it and abort
    # the in-flight block promptly, not leave the unacked block parked for the whole flush interval.
    table = "rabbitmq_cancel_wake"
    exchange = "cancel_wake_exchange"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_flush_interval_ms = 30000,
                     rabbitmq_max_block_size = 1000,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    # Fewer than a full block: the source reads them, then parks in waitForMessages waiting up to
    # rabbitmq_flush_interval_ms (30s) for the block to fill.
    publish(rabbitmq_cluster, exchange, 0, 3)
    time.sleep(5)  # let the source read the rows and settle into the flush wait

    # CANCEL must wake that wait and abort the in-flight block well within the 30s flush interval.
    t0 = time.time()
    instance.query(f"SYSTEM CANCEL test.{table}")
    instance.wait_for_log_line(
        f"{table}.*Consumption interrupted: discarding in-flight block",
        look_behind_lines=5000,
        timeout=45,
    )
    elapsed = time.time() - t0
    assert elapsed < 10, (
        f"SYSTEM CANCEL took {elapsed:.1f}s to abort the in-flight block; it should wake the flush "
        "wait promptly instead of waiting out rabbitmq_flush_interval_ms"
    )


def test_drop_during_cancel_all_background_no_race(rabbitmq_cluster):
    # Regression for the shutdown() vs cancelBackgroundActivity() data race on consumers_ref:
    # DROP TABLE runs shutdown() (which clears consumers_ref) while SYSTEM CANCEL ALL BACKGROUND
    # iterates consumers_ref. Hammer the two concurrently; the server must stay up. The underlying
    # UB is what ThreadSanitizer/stress CI catches; here we assert no crash.
    stop = threading.Event()

    def spam_cancel_all_background():
        # CANCEL ALL BACKGROUND fans out cancelBackgroundActivity() over every live streaming table
        # without blocking consumption, so it races shutdown() but does not stall table setup.
        while not stop.is_set():
            instance.query("SYSTEM CANCEL ALL BACKGROUND")

    hammer = threading.Thread(target=spam_cancel_all_background, daemon=True)
    hammer.start()
    try:
        for i in range(30):
            table = f"rabbitmq_drop_race_{i}"
            exchange = f"drop_race_exchange_{i}"
            setup_consuming_table(table, exchange)
            publish(rabbitmq_cluster, exchange, 0, 5)
            # DROP triggers flushAndShutdown() -> shutdown() while the table is still registered, so
            # the concurrent CANCEL ALL BACKGROUND fan-out can hit it mid-shutdown.
            instance.query(f"DROP TABLE test.{table}_mv SYNC")
            instance.query(f"DROP TABLE test.{table} SYNC")
            instance.query(f"DROP TABLE test.{table}_dst SYNC")
    finally:
        stop.set()
        hammer.join()

    assert instance.query("SELECT 1") == "1\n"


def test_cancel_requeues_unhandled_messages_on_abort(rabbitmq_cluster):
    # An aborted block must be requeued in full, including malformed messages reject_unhandled_messages
    # put in failed_delivery_tags; the bug rejected those without requeue and dropped them. In 'stream'
    # mode a malformed message is an error row, so it survives abort + redelivery only if requeued too.
    table = "rabbitmq_reject_abort"
    exchange = "reject_abort_exchange"
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{exchange}',
                     rabbitmq_handle_error_mode = 'stream',
                     reject_unhandled_messages = 1,
                     rabbitmq_flush_interval_ms = 5000,
                     rabbitmq_max_block_size = 1000,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, error Nullable(String))
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, _error AS error FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    # Pre-load five good rows + one malformed while halted, then resume so a fresh cycle reads all six
    # into one block and holds it open ~rabbitmq_flush_interval_ms (read done, nothing acked yet).
    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 1, 5)
    publish_raw(rabbitmq_cluster, exchange, "not a json")
    instance.query(f"SYSTEM START test.{table}")

    time.sleep(2)  # the block is open at the source: all six read, nothing acked yet
    instance.query(f"SYSTEM CANCEL test.{table}")  # aborts the block -> nackMessages(requeue=true)

    # CANCEL keeps consuming, so the requeued block is redelivered. All six must land (five good + the
    # malformed error row); the bug dropped the malformed tag, so its row never reappears.
    wait_dst_count(table, 6)
    assert (
        int(instance.query(f"SELECT count() FROM test.{table}_dst WHERE error IS NOT NULL")) == 1
    ), "the malformed message must be requeued on abort, not dropped"


def test_refresh_survives_unready_dependencies(rabbitmq_cluster):
    # A REFRESH issued while the dependent view is attached but not ready (its target table
    # detached) must run once the target returns, not be lost: a streaming round consumed the
    # one-shot permit and then bailed on the dependency check before any streaming.
    table = "rabbitmq_refresh_unready"
    exchange = "refresh_unready_exchange"
    setup_consuming_table(table, exchange)

    publish(rabbitmq_cluster, exchange, 0, 1)
    wait_dst_count(table, 1)

    instance.query(f"SYSTEM STOP test.{table}")
    publish(rabbitmq_cluster, exchange, 1, 5)
    assert_dst_count_stable(table, 1)

    instance.query(f"DETACH TABLE test.{table}_dst")
    instance.query(f"SYSTEM REFRESH test.{table}")
    time.sleep(2)  # streaming rounds wake with the view unready and must keep the permit

    # Once the target table is back, the queued REFRESH must still drain the queued backlog.
    instance.query(f"ATTACH TABLE test.{table}_dst")
    wait_dst_count(table, 6)
    assert_dst_count_stable(table, 6, seconds=5)
