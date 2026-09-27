import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_with_backoff = cluster.add_instance(
    "node_with_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/backoff_policy.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node_no_backoff = cluster.add_instance(
    "node_no_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/no_backoff_policy.xml"],
    with_zookeeper=True,
)

REPLICATED_POSTPONE_LOG = (
    "According to exponential backoff policy, put aside this log entry"
)
NON_REPLICATED_POSTPONE_MUTATION_LOG = (
    "According to exponential backoff policy, do not perform mutations for the part"
)
NON_REPLICATED_POSTPONE_MERGE_LOG = (
    "According to exponential backoff policy, do not perform merges for the part"
)
FAILED_MERGE_LOG = "Exception is in merge_task."
FAILING_MUTATION_QUERY = "ALTER TABLE test_table DELETE WHERE x IN (SELECT throwIf(1)) SETTINGS allow_nondeterministic_mutations = 1"

# Bounds for the merge retry rate over one window. With the backoff, the delay is at its 60s cap
# by then, so no two failures fit into one window however slow the machine is. Without it, 92
# and 102 were measured on an idle 96-core machine, and the unpaced bound is about 8x below that
# because this module is re-run under ASan and UBSan, where the unpaced cycle is dominated by
# exception construction and stack-trace symbolization.
RATE_WINDOW_SECONDS = 60
PACED_MAX_FAILURES = 1
UNPACED_MIN_FAILURES = 12

all_nodes = [node_with_backoff, node_no_backoff]


def prepare_cluster(use_replicated_table):
    for node in all_nodes:
        node.query("DROP TABLE IF EXISTS test_table SYNC")

    engine = (
        "ReplicatedMergeTree('/clickhouse/{cluster}/tables/test/test_table', '{instance}')"
        if use_replicated_table
        else "MergeTree()"
    )

    for node in all_nodes:
        node.rotate_logs()
        node.query(f"CREATE TABLE test_table(x UInt32) ENGINE {engine} ORDER BY x")
        node.query("INSERT INTO test_table SELECT * FROM numbers(10) ORDER BY ALL")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    ("node, found_in_log"),
    [
        (
            node_with_backoff,
            True,
        ),
        (
            node_no_backoff,
            False,
        ),
    ],
)
def test_mutation_exponential_backoff_with_merge_tree(
    started_cluster, node, found_in_log
):
    prepare_cluster(False)

    def check_logs():
        if found_in_log:
            assert node.wait_for_log_line(NON_REPLICATED_POSTPONE_MUTATION_LOG)
            # Do not rotate the logs when we are checking the absence of a log message
            node.rotate_logs()
        else:
            # Best effort, but when it fails, then the logs for sure contain the problematic message
            assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MUTATION_LOG)

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)

    check_logs()

    node.query("KILL MUTATION WHERE table='test_table'")
    # Check that after kill new parts mutations are postponing.
    node.query(FAILING_MUTATION_QUERY)

    check_logs()


def prepare_table_with_failing_merge(node, extra_settings=None):
    """A non-replicated table whose every merge fails, as in issue #120263.

    The column type is changed to one the stored values do not fit, which is accepted as a
    metadata change. Merges read through the part's own type and cast to the type in the
    metadata, so the conversion, and with it the merge, fails every time.
    """
    # Drop before rotating, so that a postpone line logged for the previous table cannot land
    # in the fresh log and defeat an absence assertion made against it. Both nodes are cleaned,
    # so that no task still failing on the other one runs alongside the timing checks.
    for n in all_nodes:
        n.query("DROP TABLE IF EXISTS test_table SYNC")
    node.rotate_logs()
    # merge_selector_base=1 makes the two equal-sized parts mergeable straight away instead
    # of only once they have aged.
    settings = ["merge_selector_base = 1"] + (extra_settings or [])
    node.query(
        "CREATE TABLE test_table(x UInt32, s String) ENGINE=MergeTree() ORDER BY x "
        "SETTINGS " + ", ".join(settings)
    )
    # Nothing may merge before the type is changed, otherwise a single part is left and no
    # merge is ever attempted afterwards.
    node.query("SYSTEM STOP MERGES test_table")
    node.query(
        "INSERT INTO test_table SELECT number, 'v' || toString(number) FROM numbers(10)"
    )
    node.query(
        "INSERT INTO test_table SELECT number + 10, 'v' || toString(number) FROM numbers(10)"
    )
    node.query(
        "ALTER TABLE test_table MODIFY COLUMN s Enum16('v0' = 0) "
        "SETTINGS alter_sync = 0, mutations_sync = 0"
    )
    # The conversion mutation fails for the same reason and arms the mutation backoff, which
    # logs a postpone line of its own. Killing it leaves the merge as the only failing task.
    node.query("KILL MUTATION WHERE table = 'test_table' SYNC")
    node.query("SYSTEM START MERGES test_table")


@pytest.mark.parametrize(
    ("node, found_in_log"),
    [
        (
            node_with_backoff,
            True,
        ),
        (
            node_no_backoff,
            False,
        ),
    ],
)
def test_merge_exponential_backoff_with_merge_tree(started_cluster, node, found_in_log):
    prepare_table_with_failing_merge(node)

    # The backoff starts at 2ms and doubles per failure until it reaches the 60s cap, so from
    # the 16th failure on, as 2^16 ms is more than 60s, the delay is at its ceiling. Wait
    # for that many on both nodes, so that both the absence check and the rate window below
    # are made after the same amount of merge activity and with the ladder fully climbed. The
    # ladder itself takes about 66s, and the loop exits as soon as the count is reached, so the
    # timeout below is sized for the ASan and UBSan lane rather than for the ladder.
    start_time = time.monotonic()
    while (
        int(node.count_in_log(FAILED_MERGE_LOG)) < 16
        and time.monotonic() < start_time + 420
    ):
        time.sleep(1)
    assert int(node.count_in_log(FAILED_MERGE_LOG)) >= 16

    if found_in_log:
        assert node.wait_for_log_line(NON_REPLICATED_POSTPONE_MERGE_LOG)
        # A failed merge postpones every one of its source parts.
        parts = node.query(
            "SELECT name FROM system.parts WHERE database = currentDatabase() "
            "AND table = 'test_table' AND active"
        ).split()
        assert len(parts) == 2
        for part in parts:
            assert node.contains_in_log(
                f"do not perform merges for the part {part} yet"
            ), f"part {part} was not postponed"
    else:
        # Best effort, but when it fails, then the logs for sure contain the problematic message
        assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MERGE_LOG)

    # The postpone line alone does not pin the retry RATE, which is the only thing the policy
    # changes for the user: a policy that rejected one selection and then retried at a small
    # constant delay would log it just the same. So count the failures over one window.
    before = int(node.count_in_log(FAILED_MERGE_LOG))
    time.sleep(RATE_WINDOW_SECONDS)
    delta = int(node.count_in_log(FAILED_MERGE_LOG)) - before
    if found_in_log:
        assert (
            delta <= PACED_MAX_FAILURES
        ), f"backoff did not pace the retries: {delta} in {RATE_WINDOW_SECONDS}s"
    else:
        assert (
            delta >= UNPACED_MIN_FAILURES
        ), f"retries stopped with the backoff off: {delta} in {RATE_WINDOW_SECONDS}s"

    if found_in_log:
        # Do not rotate the logs when we are checking the absence of a log message
        node.rotate_logs()

    # An explicit OPTIMIZE is never postponed: the user gets the merge's own error.
    assert "UNKNOWN_ELEMENT_OF_ENUM" in node.query_and_get_error(
        "OPTIMIZE TABLE test_table FINAL"
    )

    if found_in_log:
        # Lowering the cap has to reach the postponed parts at once, not only when their delay
        # is over. So it is lowered right after a background failure has restarted the 60s delay.
        failures = len(failure_times_ms(node, "test_table", "MergeParts"))
        restarted = wait_for_more_merge_failures(node, failures, timeout=120)
        assert restarted > failures
        node.query(
            "ALTER TABLE test_table MODIFY SETTING max_postpone_time_for_failed_merges_ms = 0"
        )
        assert (
            wait_for_more_merge_failures(node, restarted, timeout=30) > restarted
        ), "lowering the cap did not reach the postponed parts"


def test_merge_backoff_cap_is_read_on_every_failure(started_cluster):
    node = node_with_backoff
    # The parts start failing with the backoff disabled, so nothing postpones them ...
    prepare_table_with_failing_merge(
        node, extra_settings=["max_postpone_time_for_failed_merges_ms = 0"]
    )

    start_time = time.monotonic()
    while (
        int(node.count_in_log(FAILED_MERGE_LOG)) < 10
        and time.monotonic() < start_time + 60
    ):
        time.sleep(1)
    assert int(node.count_in_log(FAILED_MERGE_LOG)) >= 10
    assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MERGE_LOG)

    # ... and raising the cap afterwards has to reach parts that have already failed.
    node.query(
        "ALTER TABLE test_table MODIFY SETTING max_postpone_time_for_failed_merges_ms = 60000"
    )
    assert node.wait_for_log_line(NON_REPLICATED_POSTPONE_MERGE_LOG, timeout=120)

    node.query("DROP TABLE test_table SYNC")


def failure_times_ms(node, table, event_type):
    node.query("SYSTEM FLUSH LOGS")
    return [
        int(t)
        for t in node.query(
            "SELECT toUnixTimestamp64Milli(event_time_microseconds) FROM system.part_log "
            "WHERE table_uuid = (SELECT uuid FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{table}') "
            f"AND event_type = '{event_type}' AND error != 0 ORDER BY event_time_microseconds"
        ).split()
    ]


def wait_for_more_merge_failures(node, failures, timeout):
    """Returns the number of failed merges of test_table once it exceeds `failures`, or at the timeout."""
    start_time = time.monotonic()
    while True:
        count = len(failure_times_ms(node, "test_table", "MergeParts"))
        if count > failures or time.monotonic() > start_time + timeout:
            return count
        time.sleep(1)


def test_backoff_reaches_the_configured_cap(started_cluster):
    node = node_with_backoff
    # 8000 is not a power of two, so a delay that stops at the largest power of two below the
    # cap, 4096 ms, is told apart from one that reaches it. Merges and mutations share this.
    prepare_table_with_failing_merge(
        node, extra_settings=["max_postpone_time_for_failed_merges_ms = 8000"]
    )
    try:
        node.query("DROP TABLE IF EXISTS test_table_mutation SYNC")
        node.query(
            "CREATE TABLE test_table_mutation(x UInt32) ENGINE=MergeTree() ORDER BY x "
            "SETTINGS max_postpone_time_for_failed_mutations_ms = 8000"
        )
        node.query("INSERT INTO test_table_mutation SELECT * FROM numbers(10)")
        node.query(
            "ALTER TABLE test_table_mutation DELETE WHERE x IN (SELECT throwIf(1)) "
            "SETTINGS allow_nondeterministic_mutations = 1"
        )

        # The delay after the n-th failure is 2^n ms until it reaches the cap, which it does
        # from the 13th failure on. A failure is logged before its delay starts, so a delay at
        # the cap is never shorter than 8000 ms, and the lower bound sits just below it, far
        # above a 4096 ms delay plus scheduling. From there on the delay stops doubling.
        for table, event_type in (
            ("test_table", "MergeParts"),
            ("test_table_mutation", "MutatePart"),
        ):
            start_time = time.monotonic()
            while (
                len(failure_times_ms(node, table, event_type)) < 16
                and time.monotonic() < start_time + 420
            ):
                time.sleep(1)
            times = failure_times_ms(node, table, event_type)
            assert len(times) >= 16
            delays = [times[i + 1] - times[i] for i in range(12, 15)]
            assert (
                min(delays) > 7500
            ), f"{event_type} delay stays below the cap: {delays} ms"
            assert max(delays) < 1.5 * min(
                delays
            ), f"{event_type} delay grows past the cap: {delays} ms"
    finally:
        node.query("DROP TABLE IF EXISTS test_table_mutation SYNC")
        node.query("DROP TABLE IF EXISTS test_table SYNC")


def count_postponed_tasks_in_replicated_queue(node):
    return int(
        node.query(
            f"SELECT count() FROM system.replication_queue WHERE table='test_table' and postpone_reason LIKE '%{REPLICATED_POSTPONE_LOG}%'"
        ).split()[0]
    )


@pytest.mark.parametrize(
    ("src_node, dst_node, backoff_expected"),
    [
        pytest.param(node_no_backoff, node_with_backoff, True, id="with_backoff"),
        pytest.param(node_with_backoff, node_no_backoff, False, id="without_backoff"),
    ],
)
def test_fetch_exponential_backoff_with_replicated_tree(
    started_cluster, src_node, dst_node, backoff_expected
):

    prepare_cluster(True)
    src_node.query("SYSTEM STOP MERGES test_table")
    dst_node.query("SYSTEM STOP MERGES test_table")
    dst_node.query("SYSTEM STOP FETCHES test_table")

    src_node.query("INSERT INTO test_table SELECT rand()%20 FROM numbers(10)")
    src_node.query("DETACH TABLE test_table")
    dst_node.query("SYSTEM START FETCHES test_table")

    ## The fetch from the src replica will be impossible, until table is detached.
    ## Actually this is an imitation of scenario when one replica inserted the data and immediately becomes unavaliable,
    ## so fethes are impossible.
    start_time = time.monotonic()
    task_posponed = False
    while time.monotonic() < start_time + 80:
        time.sleep(1)
        if count_postponed_tasks_in_replicated_queue(dst_node):
            task_posponed = True
            break

    assert task_posponed == backoff_expected
    src_node.query("ATTACH TABLE test_table")


def test_mutation_exponential_backoff_with_replicated_tree(started_cluster):
    prepare_cluster(True)

    node_with_backoff.query(FAILING_MUTATION_QUERY)

    assert node_with_backoff.wait_for_log_line(REPLICATED_POSTPONE_LOG)
    assert count_postponed_tasks_in_replicated_queue(node_with_backoff) == 1
    assert not node_no_backoff.contains_in_log(REPLICATED_POSTPONE_LOG)


def test_exponential_backoff_setting_override(started_cluster):
    node = node_with_backoff
    node.rotate_logs()
    node.query("DROP TABLE IF EXISTS test_table SYNC")
    node.query(
        "CREATE TABLE test_table(x UInt32) ENGINE=MergeTree() ORDER BY x SETTINGS max_postpone_time_for_failed_mutations_ms=0"
    )
    node.query("INSERT INTO test_table SELECT * FROM system.numbers LIMIT 10")

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)
    assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MUTATION_LOG)


@pytest.mark.parametrize(
    ("replicated_table"),
    [
        (False),
        (True),
    ],
)
def test_backoff_clickhouse_restart(started_cluster, replicated_table):
    prepare_cluster(replicated_table)
    node = node_with_backoff

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )

    node.restart_clickhouse()
    node.rotate_logs()

    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )


@pytest.mark.parametrize(
    ("replicated_table"),
    [
        (False),
        (True),
    ],
)
def test_no_backoff_after_killing_mutation(started_cluster, replicated_table):
    prepare_cluster(replicated_table)
    node = node_with_backoff

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)

    # Executing correct mutation.
    node.query("ALTER TABLE test_table DELETE WHERE x=1")
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )
    mutation_ids = node.query("select mutation_id from system.mutations").split()

    node.query(
        f"KILL MUTATION WHERE table = 'test_table' AND mutation_id = '{mutation_ids[0]}'"
    )

    retry_count = 10
    for retry in range(retry_count):
        node.rotate_logs()
        try:
            node.wait_for_log_line(
                (
                    REPLICATED_POSTPONE_LOG
                    if replicated_table
                    else NON_REPLICATED_POSTPONE_MUTATION_LOG
                ),
                timeout=5,
            )
        except Exception:
            ## the log line not found.
            break

        if retry == retry_count - 1:
            assert False, "After killing the mutatuion it is still executed"
