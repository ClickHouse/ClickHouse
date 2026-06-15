"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a RabbitMQ table."""

import json
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
    instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) == expected,
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


def test_refresh_on_viewless_table_is_not_leaked(rabbitmq_cluster):
    # Regression: a SYSTEM REFRESH issued while no view is attached must consume the one-shot right
    # away. Otherwise it leaks until a view is attached and then fires one extra cycle even though
    # the table is STOPped — i.e. the loop must not gate claimCycle() behind num_views.
    table = "rabbitmq_refresh_leak"
    exchange = "refresh_leak_exchange"
    setup_consuming_table(table, exchange)

    # Warm up so the queue/consumer are established, then detach the view (num_views -> 0). The
    # rabbitmq_queue_base keeps the queue around so messages published below are retained.
    publish(rabbitmq_cluster, exchange, 0, 1)
    wait_dst_count(table, 1)
    instance.query(f"DROP TABLE test.{table}_mv")

    # REFRESH on the now view-less, STOPped table must consume the one-shot immediately.
    instance.query(f"SYSTEM STOP test.{table}")
    instance.query(f"SYSTEM REFRESH test.{table}")
    publish(rabbitmq_cluster, exchange, 1, 10)

    # Re-attach the view. The table is still STOPped, so nothing new should be consumed; a leaked
    # one-shot would drain the 10 messages here. The window is generous because the leaked cycle only
    # surfaces on the next reschedule and may need a consumer re-engagement first.
    instance.query(
        f"CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst "
        f"AS SELECT key, value FROM test.{table}"
    )
    assert_dst_count_stable(table, 1, seconds=15)

    # Sanity: START drains them, proving the messages were retained and consumable.
    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 11)


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
                     rabbitmq_flush_interval_ms = 3000,
                     rabbitmq_row_delimiter = '\\n';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")

    instance.query(f"SYSTEM STOP test.{table}")

    # First batch (queued in the broker while stopped), then one REFRESH: the single cycle (its flush
    # window is long enough to drain the small backlog) consumes exactly these messages.
    publish(rabbitmq_cluster, exchange, 0, 5)
    instance.query(f"SYSTEM REFRESH test.{table}")
    wait_dst_count(table, 5)

    # Second batch, no further REFRESH: the stream is still stopped, so REFRESH having run once does
    # not keep consuming — these messages stay queued in the broker, unread.
    publish(rabbitmq_cluster, exchange, 5, 5)
    assert_dst_count_stable(table, 5)

    # START resumes continuous consumption: the backlog drains and later batches keep being consumed
    # "forever" without any further command.
    instance.query(f"SYSTEM START test.{table}")
    wait_dst_count(table, 10)
    publish(rabbitmq_cluster, exchange, 10, 5)
    wait_dst_count(table, 15)


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
