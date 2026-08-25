"""Kafka reduces its streaming batch size after a memory limit error instead of failing forever."""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/memory_limit.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
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

SERVER_LOG = "/var/log/clickhouse-server/clickhouse-server.log"
REDUCTION_LINE = "Reading with reduced batch sizes after a memory limit error"

# Must match `max_server_memory_usage` in `configs/memory_limit.xml`. The headroom is pinned
# relative to what the server already tracks, so the fixture does not depend on the memory
# baseline of a particular build.
SERVER_MEMORY_LIMIT = 2000000000
HEADROOM_BYTES = 200 * 1024 * 1024

# Rows stay under the broker's ~1 MB per-message limit. A full block of `BLOCK_SIZE` of them needs
# several times the headroom, so the block has to shrink before the data can pass.
ROW_BYTES = 512 * 1024
MESSAGES = 256
BLOCK_SIZE = 256
POLL_SIZE = 64

# While the memory is pinned the server has no room for the test's own queries, so every
# observation made in that window is read from the server log instead.


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("SYSTEM FREE MEMORY")
    k.clean_test_database_and_topics(instance, cluster)
    yield
    instance.query("SYSTEM FREE MEMORY")


def produce_wide_messages(kafka_cluster, topic_name, count=MESSAGES):
    payload = "x" * ROW_BYTES
    k.kafka_produce(
        kafka_cluster,
        topic_name,
        ['{{"key":{},"value":"{}"}}'.format(i, payload) for i in range(count)],
    )


def pin_memory_to_headroom():
    """Leaves only `HEADROOM_BYTES` of the server memory limit free, so a full-size Kafka block
    cannot fit. Call it last: the server cannot serve much of anything afterwards."""
    tracked = int(
        instance.query(
            "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"
        ).strip()
    )
    ballast = SERVER_MEMORY_LIMIT - tracked - HEADROOM_BYTES
    assert ballast > 0, f"nothing left to pin: tracked={tracked}"
    logging.debug("Pinning %s bytes on top of %s tracked", ballast, tracked)
    instance.query(f"SYSTEM ALLOCATE MEMORY {ballast}")


def create_kafka_pipeline(topic_name, extra_settings=""):
    instance.query(f"""
        CREATE TABLE test.dst (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = {BLOCK_SIZE},
                     kafka_poll_max_batch_size = {POLL_SIZE},
                     kafka_flush_interval_ms = 60000,
                     kafka_consumer_reschedule_ms = 200{extra_settings};

        CREATE MATERIALIZED VIEW test.mv TO test.dst AS SELECT key, value FROM test.kafka;
        """)


def log_numbers(pattern, after=None):
    """Numbers matched by `pattern` in the server log, in order of appearance. With `after`, only
    the part of the log following the first occurrence of that marker is scanned."""
    scan = (
        f"awk '/{after}/ {{seen = 1}} seen' {SERVER_LOG}"
        if after
        else f"cat {SERVER_LOG}"
    )
    out = instance.exec_in_container(
        ["bash", "-c", f"{scan} | grep -oE '{pattern}' | grep -oE '[0-9]+$' || true"]
    )
    return [int(x) for x in out.split()]


def reduced_block_sizes():
    return log_numbers(f"{REDUCTION_LINE}: block size [0-9]+")


def polled_batch_sizes(after=None):
    return log_numbers("Polled batch of [0-9]+", after=after)


def memory_errors_count():
    out = instance.exec_in_container(
        ["bash", "-c", f"grep -c 'MEMORY_LIMIT_EXCEEDED' {SERVER_LOG} || true"]
    )
    return int(out.strip() or 0)


def wait_for_reductions(expected, timeout=180):
    """Returns the number of reduced-size cycles seen in the log, so a caller can assert either a
    lower bound or equality."""
    deadline = time.monotonic() + timeout
    observed = len(reduced_block_sizes())
    while observed < expected and time.monotonic() < deadline:
        time.sleep(1)
        observed = len(reduced_block_sizes())
    return observed


def wait_for_memory_errors(expected, timeout=180):
    deadline = time.monotonic() + timeout
    observed = memory_errors_count()
    while observed < expected and time.monotonic() < deadline:
        time.sleep(1)
        observed = memory_errors_count()
    return observed


def reductions_event():
    """The `KafkaBatchSizeReductions` profile event. It is server-global and accumulates across
    tests, so callers compare it against a snapshot rather than against zero. Only usable once the
    pinned memory is released."""
    return int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'KafkaBatchSizeReductions'"
        ).strip()
        or 0
    )


def drain_topic(expected_rows, retry_count=180):
    """Releases the pinned memory and waits for the reduced cycles to consume everything."""
    instance.query("SYSTEM FREE MEMORY")
    got = instance.query_with_retry(
        "SELECT count() FROM test.dst",
        retry_count=retry_count,
        sleep_time=1,
        check_callback=lambda res: int(res.strip() or 0) == expected_rows,
    )
    assert int(got.strip()) == expected_rows


def test_batch_size_is_reduced_after_memory_limit(kafka_cluster):
    instance.rotate_logs()
    topic_name = f"kafka_mem_reduce_{k.random_string(6)}"
    events_before = reductions_event()

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(topic_name)

        # The reduction itself is the oracle. A run that merely ends up green proves nothing: the
        # rows could have arrived without any block having been too large in the first place.
        assert wait_for_reductions(1) >= 1
        sizes = reduced_block_sizes()
        assert sizes[0] == BLOCK_SIZE // 2, sizes

        # The poll shrinks together with the block. Without this the block size is only a floor:
        # a block is never cut short of a polled batch, so a poll of the original size still
        # allocates the original amount.
        #
        # Not covered here: the cap also has to be installed before `subscribe()`, whose own poll
        # would otherwise be uncapped. On this fixture that poll always returns zero messages
        # (the assignment arrives later), so no assertion can distinguish the two orders.
        polled = polled_batch_sizes(after=REDUCTION_LINE)
        assert polled, "no poll was observed after the reduction"
        assert max(polled) <= POLL_SIZE // 2, polled

        # The input is not lost, which is what makes retrying a smaller unit sufficient.
        drain_topic(MESSAGES)
        # One halving per distinct size used, and the profile event agrees with the log.
        distinct = sorted(set(reduced_block_sizes()), reverse=True)
        assert reductions_event() - events_before == len(distinct), distinct


def test_reduction_is_not_restored_after_success(kafka_cluster):
    """A later successful cycle keeps the reduced size. A success cannot be attributed to the
    consumer that failed, because consumers are pooled and handed to whichever source asks for
    one, so restoring the size on a success would oscillate instead of converging."""
    instance.rotate_logs()
    topic_name = f"kafka_mem_persist_{k.random_string(6)}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(topic_name)

        assert wait_for_reductions(1) >= 1
        drain_topic(MESSAGES)
        after_drain = reductions_event()
        smallest = min(reduced_block_sizes())

        # More successful cycles: none of them may raise the level (nothing fails any more) and
        # none may lower it back.
        produce_wide_messages(kafka_cluster, topic_name, count=4)
        instance.query_with_retry(
            "SELECT count() FROM test.dst",
            retry_count=180,
            sleep_time=1,
            check_callback=lambda res: int(res.strip() or 0) == MESSAGES + 4,
        )
        assert reductions_event() == after_drain

        # A restored size would stop logging the reduced-size line, or log a larger size again.
        before = len(reduced_block_sizes())
        time.sleep(5)
        after = reduced_block_sizes()
        assert len(after) > before, (before, len(after))
        assert max(after[before:]) == smallest, after[before:]


def test_non_memory_error_does_not_reduce(kafka_cluster):
    """Pins the error-code gate: an over-broad `catch` would shrink the batch on any failure."""
    instance.rotate_logs()
    topic_name = f"kafka_mem_other_{k.random_string(6)}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        # Unparseable messages make the pipeline throw under the default handle-error mode, with a
        # parse error rather than a memory one.
        k.kafka_produce(kafka_cluster, topic_name, ["}not json at all{"] * 8)
        before = reductions_event()
        create_kafka_pipeline(topic_name)

        assert instance.wait_for_log_line("while parsing Kafka message", timeout=120)
        assert reduced_block_sizes() == []
        assert reductions_event() == before


def test_commit_every_batch_is_excluded(kafka_cluster):
    """With `kafka_commit_every_batch` the offsets of a partially consumed block are committed
    mid-block and are not rewound on a pipeline error, so the messages that never reached the views
    are gone and a smaller batch cannot bring them back. That mode therefore does not adapt.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_commit_{k.random_string(6)}"
    events_before = reductions_event()

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(
            topic_name,
            extra_settings=",\n                     kafka_commit_every_batch = 1",
        )

        # Same trigger as the positive test, so that reaching the memory error is not in question:
        # this arm asserts the reduction is skipped, not that nothing went wrong.
        assert wait_for_memory_errors(1) >= 1
        time.sleep(10)
        assert reduced_block_sizes() == []
        instance.query("SYSTEM FREE MEMORY")
        assert reductions_event() == events_before


def test_concurrent_failures_reduce_one_step_at_a_time(kafka_cluster):
    """With `kafka_thread_per_consumer` several cycles run at once and can fail at the same size.
    The level is raised by a compare-and-swap on the size the failing cycle actually used, so it
    advances one step per size and every intermediate size gets used. Adding one per failure
    instead would skip sizes, and since the reduction is never restored it would stay there.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_tpc_{k.random_string(6)}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name, num_partitions=4):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        instance.query(f"""
            CREATE TABLE test.dst (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

            CREATE TABLE test.kafka (key UInt64, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}',
                         kafka_format = 'JSONEachRow',
                         kafka_num_consumers = 4,
                         kafka_thread_per_consumer = 1,
                         kafka_max_block_size = {BLOCK_SIZE},
                         kafka_poll_max_batch_size = {POLL_SIZE},
                         kafka_flush_interval_ms = 60000,
                         kafka_consumer_reschedule_ms = 200;

            CREATE MATERIALIZED VIEW test.mv TO test.dst AS SELECT key, value FROM test.kafka;
            """)

        assert wait_for_reductions(2) >= 2
        # Let the four tasks keep failing for a while, so a per-failure increment would have room
        # to run ahead of a per-size one.
        time.sleep(20)

        sizes = reduced_block_sizes()
        logging.debug("Reduced block sizes observed: %s", sizes)
        assert sizes[0] == BLOCK_SIZE // 2, sizes
        distinct = sorted(set(sizes), reverse=True)
        expected = [BLOCK_SIZE >> i for i in range(1, len(distinct) + 1)]
        assert distinct == expected, (distinct, expected)

        drain_topic(MESSAGES)
