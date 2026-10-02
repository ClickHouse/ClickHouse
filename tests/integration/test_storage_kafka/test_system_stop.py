"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a Kafka table."""

import json
import logging
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": k.KAFKA_TOPIC_OLD,
        "kafka_group_name_old": k.KAFKA_CONSUMER_GROUP_OLD,
        "kafka_topic_new": k.KAFKA_TOPIC_NEW,
        "kafka_group_name_new": k.KAFKA_CONSUMER_GROUP_NEW,
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)

    topics = [t for t in admin_client.list_topics() if not t.startswith("_")]
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    yield


def setup_consuming_table(table, topic, keeper=False):
    # `keeper=True` stores offsets in Keeper, which selects the separate `StorageKafka2`
    # implementation (its own background task and abort path) instead of `StorageKafka`.
    keeper_settings = (
        f", kafka_keeper_path = '/clickhouse/kafka2/{table}', kafka_replica_name = 'r1'"
        if keeper
        else ""
    )
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms = 500{keeper_settings};

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """,
        settings=(
            {"allow_experimental_kafka_offsets_storage_in_keeper": 1} if keeper else {}
        ),
    )


def produce(kafka_cluster, topic, start, count):
    messages = [
        json.dumps({"key": i, "value": i}) for i in range(start, start + count)
    ]
    k.kafka_produce(kafka_cluster, topic, messages)


def wait_dst_count(table, expected):
    result = instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )
    assert int(result) == expected, f"expected {expected} rows in {table}_dst, got {result!r}"


def assert_dst_count_stable(table, expected, seconds=5):
    """The count cannot grow: check it stays at `expected` for `seconds`."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == expected
        time.sleep(1)


def count_streaming_starts(table):
    filename = "/var/log/clickhouse-server/clickhouse-server.log"
    result = instance.exec_in_container(
        ["bash", "-c", f"tail -n10000 {filename} | grep -Ec 'test.{table}.*Started streaming to 1 attached views' || true"]
    )
    return int(result.strip() or 0)


def start_and_wait_for_streaming(table, query=None):
    """SYSTEM START, then block until a *fresh* consuming cycle has begun (a new "Started streaming" past
    the current count) - so the verb lands on a claimed cycle, skipping any leftover pre-STOP one."""
    seen = count_streaming_starts(table)
    instance.query(query or f"SYSTEM START test.{table}")
    instance.wait_for_log_line(
        f"test.{table}.*Started streaming to 1 attached views", repetitions=seen + 1
    )


def produce_to_partition(kafka_cluster, topic, partition, start, count):
    """Produce to an explicit partition, so each of several consumers gets its own data."""
    producer = k.get_kafka_producer(
        kafka_cluster.kafka_port, k.producer_serializer, retries=15
    )
    for i in range(start, start + count):
        producer.send(
            topic=topic,
            value=json.dumps({"key": i, "value": i}),
            partition=partition,
        )
    producer.flush()


def read_direct(table, expected, timeout=60):
    """Accumulate rows over repeated direct SELECTs until `expected` are seen or the timeout expires.
    A direct SELECT returns whatever is available now (maybe nothing yet, before assignment), so retry."""
    total = 0
    deadline = time.time() + timeout
    while total < expected and time.time() < deadline:
        batch = int(
            instance.query(
                f"SELECT count() FROM test.{table}",
                settings={"stream_like_engine_allow_direct_select": 1},
            )
        )
        total += batch
        if batch == 0:
            time.sleep(0.5)
    return total


# The per-table verbs are parametrized over `keeper`: keeper=False exercises `StorageKafka`, keeper=True
# exercises `StorageKafka2`. May parts of code are shared, so one parametrized body covers both engines.
@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_system_stop_start_consuming(kafka_cluster, keeper):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_stop_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table, keeper=keeper)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # STOP halts consumption: new messages stay in the topic, unread.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        # START resumes consumption and the backlog is drained.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_system_pause_start_consuming(kafka_cluster, keeper):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_pause_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table, keeper=keeper)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # PAUSE disables future polling cycles but lets the one already running finish and commit.
        # Post-PAUSE records then have only future (now-disabled) cycles to land in.
        instance.query(f"SYSTEM PAUSE test.{table}")
        time.sleep(5)
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_system_cancel_consuming(kafka_cluster, keeper):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cancel_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table, keeper=keeper)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # CANCEL interrupts only the current poll; it does NOT stop further activity,
        # so consumption keeps going and later messages are still ingested.
        instance.query(f"SYSTEM CANCEL test.{table}")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_system_refresh_consuming(kafka_cluster, keeper):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refresh_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table, keeper=keeper)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # REFRESH kicks off a poll out of order; streaming continues normally.
        instance.query(f"SYSTEM REFRESH test.{table}")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_refresh_runs_once_while_start_keeps_consuming(kafka_cluster):
    # REFRESH runs exactly one polling cycle out of order without resuming the stream; START resumes
    # continuous polling. While the stream is stopped, a single REFRESH drains exactly the backlog
    # present at that moment, and messages produced afterwards stay unread until START — whereas
    # after START every later batch is consumed without any further command.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refreshonce_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 1)
        wait_dst_count(table, 1)

        instance.query(f"SYSTEM STOP test.{table}")

        # First batch, then one REFRESH: the single cycle drains exactly these rows.
        produce(kafka_cluster, table, 1, 5)
        instance.query(f"SYSTEM REFRESH test.{table}")
        wait_dst_count(table, 6)

        # Second batch, no further REFRESH: the stream is still stopped, so REFRESH having run once
        # does not keep consuming — these rows stay in the topic, unread.
        produce(kafka_cluster, table, 6, 5)
        assert_dst_count_stable(table, 6)

        # START resumes continuous polling: the backlog drains and later batches keep being consumed.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 11)
        produce(kafka_cluster, table, 11, 5)
        wait_dst_count(table, 16)


def test_stop_aborts_inflight_block_pause_commits_it(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)

    for verb in ["PAUSE", "STOP"]:
        table = f"kafka_inflight_{verb.lower()}_{k.random_string(6)}"
        with k.kafka_topic(admin_client, table):
            instance.query(
                f"""
                CREATE TABLE test.{table} (key UInt64, value UInt64)
                    ENGINE = Kafka
                    SETTINGS kafka_broker_list = 'kafka1:19092',
                             kafka_topic_list = '{table}',
                             kafka_group_name = '{table}',
                             kafka_format = 'JSONEachRow',
                             kafka_flush_interval_ms = 5000,
                             kafka_poll_timeout_ms = 1000,
                             kafka_max_block_size = 1000;
                CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
                CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                    SELECT key, value FROM test.{table};
                """
            )

            # Pre-load the topic while halted, then resume and wait for a fresh cycle to start streaming
            # the 5 rows (held ~kafka_flush_interval_ms before committing), so the verb lands mid-cycle.
            instance.query(f"SYSTEM STOP test.{table}")
            produce(kafka_cluster, table, 0, 5)
            start_and_wait_for_streaming(table)
            instance.query(f"SYSTEM {verb} test.{table}")

            if verb == "PAUSE":
                # The in-flight block finishes and commits on its own, without START.
                wait_dst_count(table, 5)
            else:
                # The in-flight block is aborted and never committed (and future cycles are blocked),
                # so the rows stay invisible until START redelivers them.
                assert_dst_count_stable(table, 0, seconds=8)
                instance.query(f"SYSTEM START test.{table}")
                wait_dst_count(table, 5)


def test_kafka2_stop_aborts_inflight_block_pause_commits_it(kafka_cluster):
    # Same in-flight STOP-vs-PAUSE distinction as the v1 test, but for `StorageKafka2`, whose durable
    # boundary is an uncommitted Keeper `offset_guard` (rolled back on abort) rather than v1's
    # `markDirty`+rewind.
    admin_client = k.get_admin_client(kafka_cluster)

    for verb in ["PAUSE", "STOP"]:
        table = f"kafka2_inflight_{verb.lower()}_{k.random_string(6)}"
        with k.kafka_topic(admin_client, table):
            instance.query(
                f"""
                CREATE TABLE test.{table} (key UInt64, value UInt64)
                    ENGINE = Kafka
                    SETTINGS kafka_broker_list = 'kafka1:19092',
                             kafka_topic_list = '{table}',
                             kafka_group_name = '{table}',
                             kafka_format = 'JSONEachRow',
                             kafka_flush_interval_ms = 5000,
                             kafka_poll_timeout_ms = 1000,
                             kafka_max_block_size = 1000,
                             kafka_keeper_path = '/clickhouse/kafka2/{table}',
                             kafka_replica_name = 'r1';
                CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
                CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                    SELECT key, value FROM test.{table};
                """,
                settings={"allow_experimental_kafka_offsets_storage_in_keeper": 1},
            )

            # Pre-load the topic while halted, then resume and wait for a fresh cycle to start streaming
            # the 5 rows (held ~kafka_flush_interval_ms before the offset guard commits) so the verb lands mid-cycle.
            instance.query(f"SYSTEM STOP test.{table}")
            produce(kafka_cluster, table, 0, 5)
            start_and_wait_for_streaming(table)
            instance.query(f"SYSTEM {verb} test.{table}")

            if verb == "PAUSE":
                # The in-flight block finishes and commits the offset guard on its own, without START.
                wait_dst_count(table, 5)
            else:
                # The in-flight block is aborted before the offset guard commits (and future cycles
                # are blocked), so the rows stay invisible until START redelivers them.
                assert_dst_count_stable(table, 0, seconds=8)
                instance.query(f"SYSTEM START test.{table}")
                wait_dst_count(table, 5)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_stop_during_insert_does_not_duplicate(kafka_cluster, keeper):
    # A STOP arriving while the block is being inserted into the views (the poll already returned it)
    # must let the insert finish and commit its offsets, so the rows land exactly once and are never
    # redelivered. The per-row-sleeping view stretches the insert; max_block_size = n makes the poll
    # return at once, so the block is already past the source when STOP arrives.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_stop_insert_{k.random_string(6)}"
    n = 5
    with k.kafka_topic(admin_client, table):
        keeper_settings = (
            f", kafka_keeper_path = '/clickhouse/kafka2/{table}', kafka_replica_name = 'r1'"
            if keeper
            else ""
        )
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500,
                         kafka_poll_timeout_ms = 1000,
                         kafka_max_block_size = {n}{keeper_settings};
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
            """,
            settings=(
                {"allow_experimental_kafka_offsets_storage_in_keeper": 1} if keeper else {}
            ),
        )

        # Halt, pre-load exactly one block, then resume: the source polls all n rows at once
        # (max_block_size = n, no flush wait) and hands them to the ~n*0.4s insert.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 0, n)
        instance.query(f"SYSTEM START test.{table}")

        # Land a STOP while the slow insert is running, then immediately START so that a block whose
        # offsets were (incorrectly) rolled back instead of committed would be redelivered as a duplicate.
        time.sleep(1)
        instance.query(f"SYSTEM STOP test.{table}")
        instance.query(f"SYSTEM START test.{table}")

        # The block commits its offsets exactly once: the rows appear and never grow past n (no
        # duplicate), and none are missing (no loss).
        wait_dst_count(table, n)
        assert_dst_count_stable(table, n, seconds=8)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_cancel_during_insert_does_not_duplicate(kafka_cluster, keeper):
    # CANCEL companion to test_stop_during_insert_does_not_duplicate: a CANCEL during the slow insert
    # must let the block finish and commit exactly once. CANCEL keeps scheduling, so a wrongly
    # rolled-back block would be redelivered on its own and show up as a duplicate (no START needed).
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cancel_insert_{k.random_string(6)}"
    n = 5
    with k.kafka_topic(admin_client, table):
        keeper_settings = (
            f", kafka_keeper_path = '/clickhouse/kafka2/{table}', kafka_replica_name = 'r1'"
            if keeper
            else ""
        )
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500,
                         kafka_poll_timeout_ms = 1000,
                         kafka_max_block_size = {n}{keeper_settings};
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table} WHERE sleepEachRow(0.4) = 0;
            """,
            settings=(
                {"allow_experimental_kafka_offsets_storage_in_keeper": 1} if keeper else {}
            ),
        )

        # Halt, pre-load exactly one block, then resume: the source polls all n rows at once
        # (max_block_size = n, no flush wait) and hands them to the ~n*0.4s insert.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 0, n)
        instance.query(f"SYSTEM START test.{table}")

        # CANCEL during the slow insert; it keeps scheduling, so a rolled-back block would redeliver.
        time.sleep(1)
        instance.query(f"SYSTEM CANCEL test.{table}")

        # Committed exactly once: count reaches n and never grows (no duplicate, no loss).
        wait_dst_count(table, n)
        assert_dst_count_stable(table, n, seconds=8)


def test_stop_with_commit_every_batch_does_not_lose_rows(kafka_cluster):
    # `kafka_commit_every_batch` commits offsets at every poll batch (`kafka_poll_max_batch_size`)
    # while the single block is still being assembled, BEFORE it is inserted into the views. 
    # SYSTEM STOP aborting in-flight block discards rows but their offsets stay committed.
    # Consumer that rejoins from the committed offset resumes PAST the discarded rows and they are lost.
    #
    # kafka_poll_max_batch_size = 1 commits after every row; kafka_max_block_size = 1000 keeps the
    # block open for ~kafka_flush_interval_ms so STOP lands while the rows are committed but not inserted.
    # v1 only: `kafka_commit_every_batch` is a `StorageKafka`/`KafkaConsumer` setting; v2 does not use it.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cebatch_{k.random_string(6)}"
    n = 10
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_commit_every_batch = 1,
                         kafka_poll_max_batch_size = 1,
                         kafka_max_block_size = 1000,
                         kafka_flush_interval_ms = 5000,
                         kafka_poll_timeout_ms = 1000;
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )

        produce(kafka_cluster, table, 0, 1)
        wait_dst_count(table, 1)

        # Halt, pre-load one block of n rows, then resume so a fresh cycle opens a block, polls all n
        # rows one batch at a time -- committing each batch's offset to the broker as it goes -- and
        # holds the block open for ~kafka_flush_interval_ms before inserting it.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 1, n)
        instance.query(f"SYSTEM START test.{table}")

        time.sleep(2)  # all n rows polled and their offsets committed; block still open, not yet inserted
        instance.query(f"SYSTEM STOP test.{table}")

        # The aborted block is discarded and future cycles are blocked, so only the warm-up row is visible.
        assert_dst_count_stable(table, 1, seconds=3)

        # Restart so a brand-new consumer resumes strictly from the committed offset (what any rebalance
        # or process restart does). If the mid-block commits advanced past the discarded rows, those rows
        # are gone for good and the count never reaches 1 + n.
        instance.restart_clickhouse()
        wait_dst_count(table, 1 + n)
        assert_dst_count_stable(table, 1 + n, seconds=5)


def test_system_stop_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_allbg_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # The server-wide wildcard halts consumption for every streaming table.
        instance.query("SYSTEM STOP ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        # START ALL BACKGROUND resumes every streaming table.
        instance.query("SYSTEM START ALL BACKGROUND")
        wait_dst_count(table, 20)


def test_system_pause_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_pauseall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # PAUSE ALL BACKGROUND disables future polling cycles.
        # Post-PAUSE records only have future cycles to land in.
        instance.query("SYSTEM PAUSE ALL BACKGROUND")
        time.sleep(5)
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        instance.query("SYSTEM START ALL BACKGROUND")
        wait_dst_count(table, 20)


def test_system_cancel_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cancelall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # CANCEL ALL BACKGROUND interrupts the current poll but does not halt the stream.
        instance.query("SYSTEM CANCEL ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_refresh_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refreshall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # REFRESH ALL BACKGROUND kicks off a poll for every streaming table; the stream continues.
        instance.query("SYSTEM REFRESH ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_stop_requires_grant(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_grant_{k.random_string(6)}"
    user = f"user_{table}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)
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

def test_pause_after_refresh_while_running(kafka_cluster):
    # A REFRESH issued while the stream is running cannot leave a stale one-shot
    # grant behind. If the one-shot is not consumed while running, a later PAUSE is
    # violated: the next background wake-up spends the leftover grant on one full committed
    # cycle, ingesting data that arrived after the PAUSE.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_staleref_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        # Data produced right after PAUSE sits in the topic for ~10s before the next wake-up
        # so a stale one-shot firing
        # on that wake-up is reliably observed by the stability window below.
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500,
                         kafka_consumer_reschedule_ms = 10000;
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # Plant the one-shot while the stream is running, then PAUSE before the next wake-up.
        instance.query(f"SYSTEM REFRESH test.{table}")
        instance.query(f"SYSTEM PAUSE test.{table}")

        # Let the cycle the REFRESH may have triggered finish: PAUSE lets an in-flight cycle
        # complete, and rows produced while it is still polling would legitimately land in it.
        time.sleep(2)
        produce(kafka_cluster, table, 10, 10)

        # The window outlasts the 10 s reschedule: if a stale one-shot survived the REFRESH,
        # the wake-up consumes the second batch here and the count grows past 10.
        assert_dst_count_stable(table, 10, seconds=15)

        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


@pytest.mark.parametrize("verb", ["STOP", "CANCEL"])
def test_direct_select_not_poisoned_by_stop_or_cancel(kafka_cluster, verb):
    # With no attached view no streaming cycle runs, so nothing may rely on a cycle start to reset an
    # in-flight cancel request. After STOP (which also requests a cancel) or a bare CANCEL while idle,
    # a direct SELECT must still poll and return data. Each verb uses a fresh table so its read is the
    # consumer's first group join (a second read would rebalance and flake on churn, not the cancel).
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_select_{verb.lower()}_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500;
            """
        )

        produce(kafka_cluster, table, 0, 10)

        # Issue the verb while idle (STOP also engages the blocker, so resume with START), then read.
        instance.query(f"SYSTEM {verb} test.{table}")
        if verb == "STOP":
            instance.query(f"SYSTEM START test.{table}")
        assert read_direct(table, 10) >= 10


def test_multi_consumer_stop_aborts_all_inflight_blocks(kafka_cluster):
    # With kafka_thread_per_consumer=1 each consumer runs its own task against the shared control. A
    # STOP while several cycles are in flight must abort ALL of them: no task commits its open block,
    # and no task's cycle start erases a sibling's pending request.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_multi_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table, num_partitions=2):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_num_consumers = 2,
                         kafka_thread_per_consumer = 1,
                         kafka_flush_interval_ms = 5000,
                         kafka_poll_timeout_ms = 1000,
                         kafka_max_block_size = 1000;
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )

        # Pre-load both partitions while halted, then resume so each consumer task opens its
        # own block and holds it for ~kafka_flush_interval_ms.
        instance.query(f"SYSTEM STOP test.{table}")
        produce_to_partition(kafka_cluster, table, 0, 0, 5)
        produce_to_partition(kafka_cluster, table, 1, 5, 5)
        instance.query(f"SYSTEM START test.{table}")

        time.sleep(2)  # both blocks are now open: rows polled but not yet committed
        instance.query(f"SYSTEM STOP test.{table}")

        # Neither task may commit its in-flight block.
        assert_dst_count_stable(table, 0, seconds=8)

        # Nothing was committed, so START redelivers everything.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 10)


def test_multi_consumer_refresh_while_stopped_covers_all(kafka_cluster):
    # On a STOPPED table, SYSTEM REFRESH must run one cycle for EVERY consumer, not just the one that
    # wins the shared grant. With two partitions and kafka_thread_per_consumer=1 each consumer owns one
    # partition, so a single REFRESH must drain BOTH. (v1 only: StorageKafka2 locks partitions per
    # replica, so on one instance a single consumer holds both and the test can't isolate the fix.)
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_multi_refresh_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table, num_partitions=2):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_num_consumers = 2,
                         kafka_thread_per_consumer = 1,
                         kafka_flush_interval_ms = 500,
                         kafka_max_block_size = 1000;
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )

        # Wait until both consumers have joined and each owns exactly one partition
        instance.query_with_retry(
            f"SELECT count() FROM system.kafka_consumers WHERE database = 'test' "
            f"AND table = '{table}' AND length(assignments.partition_id) = 1",
            check_callback=lambda res: int(res) == 2,
            retry_count=120,
            sleep_time=0.5,
        )

        # Warm up both partitions; each consumer drains and commits its own partition.
        produce_to_partition(kafka_cluster, table, 0, 0, 3)
        produce_to_partition(kafka_cluster, table, 1, 1000, 3)
        wait_dst_count(table, 6)

        # Stop, then load a distinct backlog on BOTH partitions while stopped.
        instance.query(f"SYSTEM STOP test.{table}")
        produce_to_partition(kafka_cluster, table, 0, 100, 5)
        produce_to_partition(kafka_cluster, table, 1, 1100, 5)
        assert_dst_count_stable(table, 6, seconds=5)

        # A single REFRESH while stopped must run a cycle for each consumer, draining both partitions.
        instance.query(f"SYSTEM REFRESH test.{table}")
        p0 = p1 = 0
        for _ in range(120):
            p0 = int(
                instance.query(
                    f"SELECT count() FROM test.{table}_dst WHERE key >= 100 AND key < 1000"
                )
            )
            p1 = int(
                instance.query(
                    f"SELECT count() FROM test.{table}_dst WHERE key >= 1100"
                )
            )
            if p0 == 5 and p1 == 5:
                break
            time.sleep(0.5)
        assert (
            p0 == 5 and p1 == 5
        ), f"single REFRESH while stopped drained only one partition: partition0={p0} partition1={p1} (expected 5 and 5)"


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_cancel_during_direct_select_does_not_drop_messages(kafka_cluster, keeper):
    # A direct SELECT with kafka_commit_on_select = 1 commits the offsets of the rows it returns. A
    # SYSTEM CANCEL that aborts an in-flight direct read must not advance the committed offset past
    # messages that were polled but discarded (never returned to the client) -- otherwise the next
    # read starts after them and they are silently lost. StorageKafka rewinds to the last committed
    # offset before the suffix commit; StorageKafka2 drops the offset guard so the suffix commit is a
    # no-op. We hammer SYSTEM CANCEL while draining via repeated direct SELECTs, then drain the
    # remainder with no cancels in flight; every produced key must surface in some SELECT result.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_direct_cancel_{'v2' if keeper else 'v1'}_{k.random_string(6)}"
    n = 20
    keeper_settings = (
        f", kafka_keeper_path = '/clickhouse/kafka2/{table}', kafka_replica_name = 'r1'"
        if keeper
        else ""
    )
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_commit_on_select = 1,
                         kafka_flush_interval_ms = 500{keeper_settings};
            """,
            settings=(
                {"allow_experimental_kafka_offsets_storage_in_keeper": 1}
                if keeper
                else {}
            ),
        )

        produce(kafka_cluster, table, 0, n)

        collected = set()

        def drain(deadline):
            while time.time() < deadline and len(collected) < n:
                res = instance.query(
                    f"SELECT key FROM test.{table}",
                    settings={"stream_like_engine_allow_direct_select": 1},
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

        drain(time.time() + 60)  # finish draining with no cancels in flight

        missing = set(range(n)) - collected
        assert not missing, (
            f"messages committed by an aborted direct read but never returned (lost): {sorted(missing)}"
        )


def test_direct_select_blocked_while_stopped_with_attached_view(kafka_cluster):
    # While a table with an attached materialized view is STOPped, a direct SELECT must still be
    # rejected: the direct-read guard depends on the attached view, not on whether streaming is
    # running. Otherwise the SELECT would consume messages meant for the view and lose them.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_stopped_direct_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # STOP halts consumption; the 10 new messages stay queued in the topic.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 10, 10)
        # Wait out several stopped polling cycles: if the guard were wrongly dropped while stopped,
        # it would reliably be dropped by the direct read below (so this is a true fail-first check).
        assert_dst_count_stable(table, 10)

        # The view is still attached, so the direct SELECT must be rejected (not steal the backlog).
        err = instance.query_and_get_error(
            f"SELECT count() FROM test.{table}",
            settings={"stream_like_engine_allow_direct_select": 1},
            timeout=60,
        )
        assert "Cannot read from StorageKafka with attached materialized views" in err

        # START drains the backlog, proving the messages were retained.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_direct_select_rejected_when_view_attached_while_stopped(kafka_cluster, keeper):
    # The direct-read guard reads the attached views live, so attaching a view to an already STOPped
    # table rejects a direct SELECT immediately, with no wait for a background tick.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_stopped_attach_{k.random_string(6)}"
    engine = "StorageKafka2" if keeper else "StorageKafka"
    keeper_settings = (
        f", kafka_keeper_path = '/clickhouse/kafka2/{table}', kafka_replica_name = 'r1'"
        if keeper
        else ""
    )
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500{keeper_settings};

            CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
                ENGINE = MergeTree ORDER BY key;
            """,
            settings=(
                {"allow_experimental_kafka_offsets_storage_in_keeper": 1} if keeper else {}
            ),
        )

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
        assert f"Cannot read from {engine} with attached materialized views" in err

        # START drains freshly produced messages into the view.
        instance.query(f"SYSTEM START test.{table}")
        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)


def test_two_queued_refreshes_run_two_cycles(kafka_cluster):
    # Regression: two SYSTEM REFRESH issued while the stopped worker sleeps must run TWO cycles, not
    # collapse into one. kafka_max_block_size=1 makes each cycle drain exactly one row, so the two
    # refreshes drain exactly two rows out of a larger backlog (only one row would mean they collapsed).
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_two_refresh_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500,
                         kafka_max_block_size = 1;
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
                SELECT key, value FROM test.{table};
            """
        )
        # Warm up so the consumer group is established, then stop.
        produce(kafka_cluster, table, 0, 1)
        wait_dst_count(table, 1)
        instance.query(f"SYSTEM STOP test.{table}")

        # Backlog larger than two; the stopped worker must not touch it without REFRESH.
        produce(kafka_cluster, table, 1, 5)
        assert_dst_count_stable(table, 1, seconds=3)

        # Two REFRESHes queued back-to-back while the worker sleeps: each must run one one-row cycle.
        instance.query(f"SYSTEM REFRESH test.{table}")
        instance.query(f"SYSTEM REFRESH test.{table}")
        wait_dst_count(table, 3)  # 1 warm-up + 2 refresh cycles; a collapse would stall at 2
        assert_dst_count_stable(table, 3, seconds=5)  # exactly two cycles, no continuous consumption

        # START drains the remaining backlog.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 6)


def test_refresh_survives_active_direct_reader(kafka_cluster):
    # A REFRESH permit claimed by a Kafka2 streaming round that then bails out on an active direct
    # reader must be re-credited, not lost: the round consumed the one-shot permit before the reader
    # handshake, so the refresh never ran once the reader drained and the table was stopped.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka2_refresh_reader_{k.random_string(6)}"
    n = 20
    with k.kafka_topic(admin_client, table):
        instance.query(
            f"""
            CREATE TABLE test.{table} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{table}',
                         kafka_group_name = '{table}',
                         kafka_format = 'JSONEachRow',
                         kafka_flush_interval_ms = 500,
                         kafka_keeper_path = '/clickhouse/kafka2/{table}',
                         kafka_replica_name = 'r1';
            CREATE TABLE test.{table}_dst (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            """,
            settings={"allow_experimental_kafka_offsets_storage_in_keeper": 1},
        )
        produce(kafka_cluster, table, 0, n)

        # No view is attached yet, so the direct read is allowed; sleepEachRow holds the reader
        # (and active_direct_readers) open for ~n*0.4s, and it commits no offsets.
        result = {}

        def run_select():
            result["answer"], result["error"] = instance.query_and_get_answer_with_error(
                f"SELECT count() FROM test.{table} WHERE NOT sleepEachRow(0.4)",
                settings={"stream_like_engine_allow_direct_select": 1},
            )

        reader = threading.Thread(target=run_select)
        reader.start()
        try:
            time.sleep(2)  # the reader is registered and pulling rows slowly
            instance.query(
                f"CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS "
                f"SELECT key, value FROM test.{table}"
            )
            instance.query(f"SYSTEM REFRESH test.{table}")
            time.sleep(2)  # streaming rounds claim the permit but bail out on the active reader
            instance.query(f"SYSTEM STOP test.{table}")  # blocks cycles and aborts the reader
        finally:
            reader.join(timeout=60)

        # Once the reader is gone, the queued REFRESH must still run one cycle despite the STOP.
        wait_dst_count(table, n)
        assert_dst_count_stable(table, n, seconds=5)


def test_refresh_survives_unready_dependencies(kafka_cluster):
    # A REFRESH issued while the dependent view is attached but not ready (its target table
    # detached) must run once the target returns, not be lost: a streaming round consumed the
    # one-shot permit and then bailed on the dependency check before any streaming.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refresh_unready_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 1)
        wait_dst_count(table, 1)

        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 1, 5)
        assert_dst_count_stable(table, 1)

        instance.query(f"DETACH TABLE test.{table}_dst")
        instance.query(f"SYSTEM REFRESH test.{table}")
        time.sleep(2)  # streaming rounds wake with the view unready and must keep the permit

        # Once the target table is back, the queued REFRESH must still run one cycle despite the STOP.
        instance.query(f"ATTACH TABLE test.{table}_dst")
        wait_dst_count(table, 6)
        assert_dst_count_stable(table, 6, seconds=5)
