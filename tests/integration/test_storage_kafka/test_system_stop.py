"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a Kafka table."""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
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
        f",\n                     kafka_keeper_path = '/clickhouse/kafka2/{table}',"
        f"\n                     kafka_replica_name = 'r1'"
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
    """Accumulate rows over repeated direct SELECTs (each returns one polled batch and
    commits its offsets) until `expected` rows have been seen or the timeout expires."""
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


@pytest.mark.parametrize("keeper", [False, True], ids=["v1", "v2"])
def test_refresh_on_viewless_table_is_not_leaked(kafka_cluster, keeper):
    # Regression: a SYSTEM REFRESH issued while no view is attached must consume the one-shot right
    # away. Otherwise it leaks until a view is attached and then fires one extra cycle even though
    # the table is STOPped — i.e. the loop must not gate shouldRunCycle() behind num_views.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refresh_leak_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table, keeper=keeper)

        # Warm up so the consumer/offsets are established, then detach the view (num_views -> 0).
        produce(kafka_cluster, table, 0, 1)
        wait_dst_count(table, 1)
        instance.query(f"DROP TABLE test.{table}_mv")

        # REFRESH on the now view-less, STOPped table must consume the one-shot immediately.
        instance.query(f"SYSTEM STOP test.{table}")
        instance.query(f"SYSTEM REFRESH test.{table}")
        produce(kafka_cluster, table, 1, 10)

        # Re-attach the view. The table is still STOPped, so nothing new should be consumed; a leaked
        # one-shot would drain the 10 messages here. The window is generous because the leaked cycle
        # only surfaces on the next reschedule and may need a consumer re-engagement first.
        instance.query(
            f"CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst "
            f"AS SELECT key, value FROM test.{table}"
        )
        assert_dst_count_stable(table, 1, seconds=15)

        # Sanity: START drains them, proving the messages were retained and consumable.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 11)


def test_refresh_runs_once_while_start_keeps_consuming(kafka_cluster):
    # REFRESH runs exactly one polling cycle out of order without resuming the stream; START resumes
    # continuous polling. With the stream STOPped, a single REFRESH drains exactly the backlog
    # present at that moment, and messages produced afterwards stay unread until START — whereas
    # after START every later batch is consumed without any further command.
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refreshonce_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        instance.query(f"SYSTEM STOP test.{table}")

        # First batch, then one REFRESH: the single cycle drains exactly these rows.
        produce(kafka_cluster, table, 0, 5)
        instance.query(f"SYSTEM REFRESH test.{table}")
        wait_dst_count(table, 5)

        # Second batch, no further REFRESH: the stream is still stopped, so REFRESH having run once
        # does not keep consuming — these rows stay in the topic, unread.
        produce(kafka_cluster, table, 5, 5)
        assert_dst_count_stable(table, 5)

        # START resumes continuous polling: the backlog drains and later batches keep being consumed
        # "forever" without any further command.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 10)
        produce(kafka_cluster, table, 10, 5)
        wait_dst_count(table, 15)


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

            # Pre-load the topic while halted, then resume so a fresh cycle opens a block over the 5
            # rows and holds it for ~kafka_flush_interval_ms (the block does not commit until then).
            instance.query(f"SYSTEM STOP test.{table}")
            produce(kafka_cluster, table, 0, 5)
            instance.query(f"SYSTEM START test.{table}")

            time.sleep(2)  # the block is now open: rows polled but not yet committed
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

            # Pre-load the topic while halted, then resume so a fresh cycle opens a block over the 5
            # rows and holds it for ~kafka_flush_interval_ms (the offset guard does not commit yet).
            instance.query(f"SYSTEM STOP test.{table}")
            produce(kafka_cluster, table, 0, 5)
            instance.query(f"SYSTEM START test.{table}")

            time.sleep(2)  # the block is open: rows polled but the offset guard is not yet committed
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
        # streaming engines are guarded by SYSTEM BACKGROUND specifically.
        instance.query(f"GRANT SYSTEM VIEWS ON test.{table} TO {user}")
        for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
            assert "ACCESS_DENIED" in instance.query_and_get_error(
                f"SYSTEM {verb} test.{table}", user=user
            )

        # SYSTEM BACKGROUND on the table is exactly the required privilege; every verb now succeeds.
        instance.query(f"GRANT SYSTEM BACKGROUND ON test.{table} TO {user}")
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
    # Regression test: on a table with no attached materialized view no streaming cycle ever
    # runs, so nothing may rely on a cycle start to reset the in-flight cancel request. After a
    # STOP (which also requests a cancel) or a bare CANCEL issued while idle, a direct SELECT must
    # still return data instead of bailing out before polling and returning nothing forever.
    #
    # Each verb uses its own fresh table so its direct read is the consumer's first group join. A
    # second direct read on the same pooled consumer re-subscribes and rebalances (revoke+rejoin),
    # which races the short query and marks the consumer dirty independently of the cancel logic
    # under test; that churn — not the cancel epoch — is what would make a later read flaky.
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
        assert read_direct(table, 10) == 10


def test_multi_consumer_stop_aborts_all_inflight_blocks(kafka_cluster):
    # With `kafka_thread_per_consumer = 1` every consumer runs its own independent background
    # task against the shared control state. A STOP issued while several cycles are in flight
    # must abort ALL of them before their offset commits: no task may commit its open block,
    # and no task's cycle start may erase the request for a sibling still in flight.
    # (The CANCEL-only analogue of this race is not deterministically testable: without the
    # blocker a finishing task is free to start a fresh cycle at any moment.)
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
