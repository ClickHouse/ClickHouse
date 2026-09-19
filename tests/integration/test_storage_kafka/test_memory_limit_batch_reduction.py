"""Kafka reduces its streaming batch size after a memory limit error instead of failing forever."""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/kafka.xml",
        "configs/memory_limit.xml",
        "configs/dead_letter_queue.xml",
    ],
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
MEMORY_LIMIT_CONFIG = "/etc/clickhouse-server/config.d/memory_limit.xml"

# Rows stay under the broker's ~1 MB per-message limit.
ROW_BYTES = 512 * 1024
MESSAGES = 256
BLOCK_SIZE = 256
POLL_SIZE = 64

# Half of what a full block of rows holds, so a full block cannot fit whatever a build flavour adds
# on top of the payload, while a halved one soon does and the reduction converges.
HEADROOM_BYTES = BLOCK_SIZE * ROW_BYTES // 2

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
    release_memory()
    k.clean_test_database_and_topics(instance, cluster)
    yield
    release_memory()


def produce_wide_messages(kafka_cluster, topic_name, count=MESSAGES):
    payload = "x" * ROW_BYTES
    k.kafka_produce(
        kafka_cluster,
        topic_name,
        ['{{"key":{},"value":"{}"}}'.format(i, payload) for i in range(count)],
    )


def server_memory_limit():
    """The limit the server is enforcing. A configured `0` is reported as what it resolved to."""
    return int(
        instance.query(
            "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'"
        ).strip()
    )


_pinned_limit = 0  # Matches `max_server_memory_usage` in `configs/memory_limit.xml`.


def set_server_memory_limit(limit):
    """`max_server_memory_usage` is applied on `SYSTEM RELOAD CONFIG`, and `0` asks for the
    automatic limit. The reload is served even while the server is over its limit, which is what
    makes releasing a pin possible at all. The new limit is asserted to be live, so an edit that
    matched nothing cannot leave the case measuring an unpinned server."""
    global _pinned_limit
    previous = _pinned_limit
    if limit == previous:
        return
    instance.replace_in_config(
        MEMORY_LIMIT_CONFIG,
        f"<max_server_memory_usage>{previous}</max_server_memory_usage>",
        f"<max_server_memory_usage>{limit}</max_server_memory_usage>",
    )
    _pinned_limit = limit
    instance.query("SYSTEM RELOAD CONFIG")
    live = server_memory_limit()
    assert (live == limit) if limit else (live > previous), (live, limit, previous)


def settled_memory_usage(samples=8, interval=1):
    """The lowest of several readings of whichever of tracked and resident memory is larger. An
    allocator holds pages for a few seconds after a case releases them, so a single reading taken
    right afterwards can be several times the settled figure and would pin a limit far too high to
    squeeze a block. Taking the lowest reading also errs on the tight side where memory only grows,
    which is what a sanitizer build does."""
    readings = []
    for _ in range(samples):
        readings.append(
            int(
                instance.query(
                    "SELECT greatest("
                    "(SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'),"
                    "(SELECT toInt64(value) FROM system.asynchronous_metrics WHERE metric = 'MemoryResident')"
                    ")"
                ).strip()
            )
        )
        time.sleep(interval)
    logging.debug("Memory usage readings: %s", readings)
    return min(readings)


def pin_memory_to_headroom():
    """Leaves only `HEADROOM_BYTES` of the server memory limit free, so a full-size Kafka block
    cannot fit. Call it last: the server cannot serve much of anything afterwards.

    The limit is pinned over what the server is using rather than set to a fixed value, because it
    is enforced against resident memory as well as against the tracked amount, and resident memory
    of a sanitizer build neither starts where a release build's does nor comes back down once the
    pressure is over."""
    usage = settled_memory_usage()
    logging.debug("Pinning the memory limit to %s over a usage of %s", usage + HEADROOM_BYTES, usage)
    set_server_memory_limit(usage + HEADROOM_BYTES)


def release_memory():
    """Back to the automatic limit. Every query the test makes for itself needs this first: a limit
    tight enough to shrink a block is also tight enough to refuse an ordinary `SELECT`."""
    set_server_memory_limit(0)


def create_kafka_pipeline(
    topic_name,
    extra_settings="",
    dst_columns="key UInt64, value String",
    mv_select="key, value",
):
    instance.query(f"""
        CREATE TABLE test.dst ({dst_columns}) ENGINE = MergeTree ORDER BY key;

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

        CREATE MATERIALIZED VIEW test.mv TO test.dst AS SELECT {mv_select} FROM test.kafka;
        """)


def log_numbers(pattern, after=None, anchored=True):
    """Numbers matched by `pattern` in the server log, in order of appearance. With `after`, only
    the part of the log following the first occurrence of that marker is scanned. `anchored` takes
    the number at the end of each match; without it, any number inside the match."""
    scan = (
        f"awk '/{after}/ {{seen = 1}} seen' {SERVER_LOG}"
        if after
        else f"cat {SERVER_LOG}"
    )
    pick = "[0-9]+$" if anchored else "[0-9]+"
    out = instance.exec_in_container(
        ["bash", "-c", f"{scan} | grep -oE '{pattern}' | grep -oE '{pick}' || true"]
    )
    return [int(x) for x in out.split()]


def reduced_block_sizes():
    return log_numbers(f"{REDUCTION_LINE}: block size [0-9]+")


def polled_batch_sizes(after=None):
    return log_numbers("Polled batch of [0-9]+", after=after)


def pushed_row_counts(after=None):
    """Rows delivered per cycle. `formatReadableQuantity` renders the count with two decimals, so
    the match stops at the decimal point and only the integer part is read."""
    return log_numbers(r"Pushing [0-9]+\.", after=after, anchored=False)


def memory_errors_count():
    """Only memory errors that ended a streaming cycle, so an unrelated allocation failing under the
    pinned tracker cannot stand in for the one the arm is waiting for. A failed cycle is reported
    through the storage's own logger, which names the table."""
    pattern = "StorageKafka \\(test\\..*MEMORY_LIMIT_EXCEEDED"
    out = instance.exec_in_container(
        ["bash", "-c", f"grep -cE '{pattern}' {SERVER_LOG} || true"]
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


def wait_for_more_reductions(seen, timeout=180):
    """Reduced-size cycles once more than `seen` of them have been logged. A fixed sleep cannot be
    used as the window: an idle cycle spends `kafka_max_failed_poll_attempts` polls of
    `stream_poll_timeout_ms` before it ends, which is several seconds."""
    deadline = time.monotonic() + timeout
    sizes = reduced_block_sizes()
    while len(sizes) <= seen and time.monotonic() < deadline:
        time.sleep(1)
        sizes = reduced_block_sizes()
    return sizes


def wait_for_polls(after, timeout=180):
    """Sizes of the polls made after the first reduction. A reduced cycle logs the marker before it
    builds its sources, so its first poll reaches the log later than the marker does."""
    deadline = time.monotonic() + timeout
    polled = polled_batch_sizes(after=after)
    while not polled and time.monotonic() < deadline:
        time.sleep(1)
        polled = polled_batch_sizes(after=after)
    return polled


def wait_for_delivery_at_reduced_size(after, timeout=180):
    """Row counts of the cycles that delivered rows after the first reduction. Read from the log
    because the memory is still pinned while this waits."""
    deadline = time.monotonic() + timeout
    delivered = [n for n in pushed_row_counts(after=after) if n > 0]
    while not delivered and time.monotonic() < deadline:
        time.sleep(1)
        delivered = [n for n in pushed_row_counts(after=after) if n > 0]
    return delivered


def messages_failed_event():
    """The `KafkaMessagesFailed` profile event. The `on_error` callback of each engine is its only
    increment site, so it counts exactly the errors that were treated as a property of a message.
    Only usable once the pinned memory is released."""
    return int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'KafkaMessagesFailed'"
        ).strip()
        or 0
    )


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
    """Releases the pinned limit and waits for the reduced cycles to consume everything."""
    release_memory()
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
        polled = wait_for_polls(after=REDUCTION_LINE)
        assert polled, "no poll was observed after the reduction"
        assert max(polled) <= POLL_SIZE // 2, polled

        # The point of the reduction: rows reach the views while the memory is still pinned, at a
        # size that fits. Without this the arm would only show that a reduction was logged and that
        # everything arrived once the pressure was gone, which a reduction that shrinks nothing
        # satisfies as well.
        delivered = wait_for_delivery_at_reduced_size(after=REDUCTION_LINE)
        assert delivered, "no cycle delivered rows at a reduced size while memory was pinned"
        # A block is only checked against its size once the polled batch it is filling is drained,
        # so it can overshoot by up to one batch short of a whole one.
        assert max(delivered) < BLOCK_SIZE // 2 + POLL_SIZE // 2, delivered

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
        got = instance.query_with_retry(
            "SELECT count() FROM test.dst",
            retry_count=180,
            sleep_time=1,
            check_callback=lambda res: int(res.strip() or 0) == MESSAGES + 4,
        )
        assert int(got.strip()) == MESSAGES + 4, got
        assert reductions_event() == after_drain

        # A restored size would stop logging the reduced-size line, or log a larger size again.
        before = len(reduced_block_sizes())
        after = wait_for_more_reductions(before)
        assert len(after) > before, (before, len(after))
        assert max(after[before:]) == smallest, after[before:]


def test_reduction_is_reset_by_reattach(kafka_cluster):
    """Re-attaching the table goes back to the configured sizes, which is the recovery the log line
    points the operator at. The level lives on the storage object, so a new one starts unreduced.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_reattach_{k.random_string(6)}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(topic_name)

        assert wait_for_reductions(1) >= 1
        drain_topic(MESSAGES)

        instance.query("DETACH TABLE test.kafka")
        instance.query("ATTACH TABLE test.kafka")
        # The old log, and with it every reduced-size line of the reduced storage object, is archived
        # away here, so any such line seen afterwards belongs to the re-attached table.
        instance.rotate_logs()

        # The memory is free again, so a full-size block fits: a correct reset logs nothing.
        produce_wide_messages(kafka_cluster, topic_name, count=4)
        got = instance.query_with_retry(
            "SELECT count() FROM test.dst",
            retry_count=180,
            sleep_time=1,
            check_callback=lambda res: int(res.strip() or 0) == MESSAGES + 4,
        )
        assert int(got.strip()) == MESSAGES + 4, got
        assert reduced_block_sizes() == []


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
        release_memory()
        assert reductions_event() == events_before


def test_commit_every_batch_keeps_previous_error_handling(kafka_cluster):
    """With `kafka_commit_every_batch` a block spans polls whose offsets are already committed and
    nothing rewinds them, so a memory error is handled by the callback as before instead of being
    rethrown: a throw would commit those rows without ever delivering them.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_commit_stream_{k.random_string(6)}"
    events_before = reductions_event()
    failed_before = messages_failed_event()

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(
            topic_name,
            extra_settings=(
                ",\n                     kafka_commit_every_batch = 1"
                ",\n                     kafka_handle_error_mode = 'stream'"
            ),
            dst_columns="key UInt64, value String, error String",
            mv_select="key, value, _error AS error",
        )

        # Same trigger as the arm above, and the only one available while the memory is pinned:
        # reaching a memory error is what the arm needs, not a block that got through.
        assert wait_for_memory_errors(1) >= 1

        release_memory()

        # The load-bearing assertion. The rethrow sits immediately ahead of this increment, so a
        # rethrown memory error leaves `KafkaMessagesFailed` untouched, and `on_error` is its only
        # increment site. No message here is malformed, so every increment is a memory error that
        # was handled as a bad message, which is the previous behaviour this mode keeps.
        assert messages_failed_event() > failed_before

        # The reduction is excluded in this mode through the other path as well.
        assert reduced_block_sizes() == []
        assert reductions_event() == events_before

        # Deliberately not asserted, so that a later reader does not "strengthen" this arm into
        # flakiness. That a cycle delivered rows, or that a substituted error row reached the
        # table: both need a full-size block to complete inside the headroom, and this mode is
        # excluded from the reduction that would make one fit. That no cycle ended with a memory
        # error: one still can, raised by the materialized-view push outside `on_error`. An exact
        # delivered row count: this mode loses a committed prefix whenever that push throws. All
        # three predate this change.


@pytest.mark.parametrize("mode", ["stream", "dead_letter_queue"])
def test_memory_error_is_not_a_bad_message(kafka_cluster, mode):
    """The handle-error modes other than the default report a bad message and keep consuming, so they
    do not rethrow. A memory limit reaches the same callback but is a state of the server, not a
    property of the message: reported that way it would replace a well-formed message with an error
    record, commit its offset and never let the size adapt. Both non-default modes are covered
    because the guard sits ahead of all of them.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_{mode}_{k.random_string(6)}"
    reductions_before = reductions_event()
    failed_before = messages_failed_event()
    streams_error = mode == "stream"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        pin_memory_to_headroom()
        create_kafka_pipeline(
            topic_name,
            extra_settings=f",\n                     kafka_handle_error_mode = '{mode}'",
            # Only the `stream` mode exposes the error through a virtual column; in
            # `dead_letter_queue` mode `on_error` produces no row at all.
            dst_columns="key UInt64, value String, error String"
            if streams_error
            else "key UInt64, value String",
            mv_select="key, value, _error AS error" if streams_error else "key, value",
        )

        assert wait_for_reductions(1) >= 1
        sizes = reduced_block_sizes()
        assert sizes[0] == BLOCK_SIZE // 2, sizes

        # Read once the ballast is gone but before the drain: in `dead_letter_queue` mode a
        # swallowed message produces no row at all, so the drain would time out first and hide
        # which of the two properties broke.
        release_memory()
        assert messages_failed_event() == failed_before, messages_failed_event() - failed_before

        drain_topic(MESSAGES)

        # Not a row count: a message accounted for here has been consumed as malformed, whether or
        # not its error record survived. `on_error` is the only increment site, and no message in
        # this arm is malformed, so any increment is a memory error taken for one.
        assert messages_failed_event() == failed_before, messages_failed_event() - failed_before
        if streams_error:
            assert (
                int(instance.query("SELECT count() FROM test.dst WHERE error != ''").strip()) == 0
            )
        else:
            instance.query("SYSTEM FLUSH LOGS")
            assert (
                int(
                    instance.query(
                        "SELECT count() FROM system.dead_letter_queue WHERE table = 'kafka'"
                    ).strip()
                    or 0
                )
                == 0
            )
        assert reductions_event() - reductions_before >= 1


def test_concurrent_failures_reduce_one_step_at_a_time(kafka_cluster):
    """With `kafka_thread_per_consumer` several cycles run at once and can fail at the same size.
    The level is raised by a compare-and-swap on the size the failing cycle actually used, so it
    advances one step per size and every intermediate size gets used. Adding one per failure
    instead would skip sizes, and since the reduction is never restored it would stay there.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_tpc_{k.random_string(6)}"
    events_before = reductions_event()

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
        # One halving per size used, whatever the order the four tasks failed in. Several failures at
        # one size raise the level once, so counting them instead would exceed the sizes observed.
        # Two cycles have to observe the same level for that to be visible at all, which is why the
        # fixture drives four consumers from one pinned tracker rather than one.
        reductions = reductions_event() - events_before
        assert reductions == len(distinct), (reductions, distinct)


def test_memory_error_on_select_names_the_message(kafka_cluster):
    """A direct `SELECT` runs the same callback but has no background cycle around it to record the
    failure, so the exception it raises is the only report: it has to name the message being read
    and reach the per-consumer buffer that `system.kafka_consumers` exposes.
    """
    instance.rotate_logs()
    topic_name = f"kafka_mem_select_{k.random_string(6)}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        produce_wide_messages(kafka_cluster, topic_name)
        # No materialized view: reading from a Kafka table that has one is rejected outright.
        instance.query(f"""
            CREATE TABLE test.kafka (key UInt64, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}',
                         kafka_format = 'JSONEachRow',
                         kafka_max_block_size = {BLOCK_SIZE},
                         kafka_poll_max_batch_size = {POLL_SIZE},
                         kafka_flush_interval_ms = 60000;
            """)

        # A block is never cut short of a polled batch, so a limit below the size of one batch is
        # reached while the batch is being parsed, which is where the callback runs.
        error = instance.query_and_get_error(
            "SELECT sum(length(value)) FROM test.kafka"
            f" SETTINGS max_memory_usage = {POLL_SIZE * ROW_BYTES // 2}"
        )
        logging.debug("Direct select error: %s", error)
        assert "MEMORY_LIMIT_EXCEEDED" in error, error
        # The per-query tracker, not the server-wide one, which is raised before any message is read
        # and would leave the assertions below measuring a query that never reached the callback.
        assert "Query memory limit exceeded" in error, error
        assert "while parsing Kafka message (topic:" in error, error

        recorded = instance.query(
            "SELECT countIf(arrayExists(x -> position(x, 'while parsing Kafka message') > 0,"
            " `exceptions.text`)) FROM system.kafka_consumers WHERE table = 'kafka'"
        ).strip()
        assert int(recorded) >= 1, instance.query(
            "SELECT * FROM system.kafka_consumers FORMAT Vertical"
        )
