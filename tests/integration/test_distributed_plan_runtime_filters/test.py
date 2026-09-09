"""Runtime-filter transport on a 3-node cluster.

Shuffle joins ship partials through the merge tree (build -> `rf_merge_*` -> probe). Result
equals the same query with `make_distributed_plan` off. Per-task `RuntimeFilterState*`
ProfileEvents (`system.query_log`; workers share the initiator `initial_query_id`) pin stream
and byte counts: `2 * N` states for `N` build and `N` probe tasks (all-to-all was `N * N`).
A send is counted where the state is serialized, but the root skips serializing when every probe
task has already closed its receive branch, so send counts are bounded rather than exact. Through
the merge tree every state normally arrives, while the root's broadcast to the probe tasks is
best-effort by design, so its arrivals are only bounded. A receiver consumes at most
`RUNTIME_FILTER_MERGE_FAN_IN` (16) states, including through a
2-level tree (`N = 32` -> 2 first-level merges -> root). Peak merge-task memory compared
across `N = 2 / 8 / 32` with equal bloom sizes. Persisted exchanges, cancellation, and LIMIT
early-close are separate cases.
"""

import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

pytestmark = pytest.mark.timeout(600)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
)

NODES = [node1, node2, node3]
INITIATOR = node1

# Matches `RUNTIME_FILTER_MERGE_FAN_IN` in `MergeRuntimeFiltersStep.h`.
FAN_IN = 16

BIG_ROWS = 512_000
SMALL_ROWS = 64_000
TINY_ROWS = 20_000

# 2 MiB bloom states, so the byte counters and task memory measure an equally sized payload for
# every build task, independent of how many rows its bucket scanned.
BLOOM_BYTES = 2 * 1024 * 1024


def _dist_settings(buckets, extra=""):
    settings = ", ".join(
        [
            "enable_analyzer = 1",
            "make_distributed_plan = 1",
            "enable_parallel_replicas = 0",
            f"distributed_plan_default_shuffle_join_bucket_count = {buckets}",
            f"distributed_plan_default_reader_bucket_count = {buckets}",
            "distributed_plan_max_rows_to_broadcast = 0",
            "query_plan_join_swap_table = 'false'",
            "query_plan_optimize_join_order_limit = 0",
            "query_plan_optimize_join_order_randomize = 0",
            "enable_join_runtime_filters = 1",
            "distributed_plan_join_runtime_filters = 1",
        ]
    )
    if extra:
        settings += ", " + extra
    return settings


# The exact-values limit is tiny, so every non-empty partial degrades to the settings-sized
# bloom filter: an equal payload per build task. The always-true predicate on the build side
# keeps the tiny limit authoritative: it denies the plan a cardinality estimate (an unindexed
# filter on a >50000-row scan), and without an estimate the transported geometry stays at the
# settings, for variable-width and fixed-width keys alike.
BLOOM_STATE_SETTINGS = (
    f"join_runtime_filter_exact_values_limit = 10, join_runtime_bloom_filter_bytes = {BLOOM_BYTES}"
)

JOIN_QUERY = "SELECT count() FROM big, small WHERE bid = sid AND name != 'no such name'"
JOIN_EXPECTED = str(SMALL_ROWS)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _create_tables_and_load_data()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables_and_load_data():
    for node in NODES:
        node.query(
            """
            CREATE TABLE big (bid String, v UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/big', '{replica}')
            ORDER BY bid
            """
        )
        # index_granularity = 256 gives the small table enough marks (250) for even 32 reader
        # buckets to each scan a non-empty slice with more than 10 distinct keys.
        node.query(
            """
            CREATE TABLE small (sid String, name String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/small', '{replica}')
            ORDER BY sid SETTINGS index_granularity = 256
            """
        )

    # More distinct keys than the default 10000-row exact-values limit, but with actual key bytes
    # far below the settings bloom size, for `test_short_string_keys_arrive_exact`.
    for node in NODES:
        node.query(
            """
            CREATE TABLE tiny (tid String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/tiny', '{replica}')
            ORDER BY tid SETTINGS index_granularity = 256
            """
        )

    # Every small key `number * 4 < 512000` matches exactly one big row.
    INITIATOR.query(
        f"INSERT INTO big SELECT toString(number), number FROM numbers({BIG_ROWS})"
    )
    INITIATOR.query(
        f"INSERT INTO small SELECT toString(number * 4), toString(number) FROM numbers({SMALL_ROWS})"
    )
    INITIATOR.query(
        f"INSERT INTO tiny SELECT toString(number * 4) FROM numbers({TINY_ROWS})"
    )

    for node in NODES:
        node.query("SYSTEM SYNC REPLICA big")
        node.query("SYSTEM SYNC REPLICA small")
        node.query("SYSTEM SYNC REPLICA tiny")
        assert int(node.query("SELECT count() FROM big").strip()) == BIG_ROWS
        assert int(node.query("SELECT count() FROM small").strip()) == SMALL_ROWS
        assert int(node.query("SELECT count() FROM tiny").strip()) == TINY_ROWS


def _collect_task_rows(query_id):
    """Task rows of one distributed query, from `system.query_log` of every node: worker tasks
    carry the initiator's query id as `initial_query_id` and the task id as `query`."""
    rows = []
    for node in NODES:
        node.query("SYSTEM FLUSH LOGS query_log")
        result = node.query(
            f"""
            SELECT
                query,
                ProfileEvents['RuntimeFilterStatesSent'],
                ProfileEvents['RuntimeFilterStatesReceived'],
                ProfileEvents['RuntimeFilterStateBytesSent'],
                ProfileEvents['RuntimeFilterStateBytesReceived'],
                ProfileEvents['RuntimeFilterOversizedStatesRejected'],
                memory_usage
            FROM system.query_log
            WHERE type = 'QueryFinish' AND initial_query_id = '{query_id}'
                AND query_id != initial_query_id AND event_date >= yesterday()
            """
        )
        for line in result.strip().splitlines():
            fields = line.split("\t")
            rows.append(
                {
                    "task": fields[0],
                    "states_sent": int(fields[1]),
                    "states_received": int(fields[2]),
                    "bytes_sent": int(fields[3]),
                    "bytes_received": int(fields[4]),
                    "oversized_rejected": int(fields[5]),
                    "memory_usage": int(fields[6]),
                    "node": node.name,
                }
            )
    return rows


def _run_and_collect(query, buckets, extra_settings=BLOOM_STATE_SETTINGS):
    query_id = f"rf_tree_{uuid.uuid4().hex}"
    result = INITIATOR.query(
        f"{query} SETTINGS {_dist_settings(buckets, extra_settings)}",
        query_id=query_id,
    ).strip()
    tasks = _collect_task_rows(query_id)
    return result, tasks


def _check_topology(tasks, buckets, levels):
    """Exact stream counts of the merge tree and the per-receiver fan-in bound."""
    merge_tasks = [t for t in tasks if t["task"].startswith("rf_merge_")]
    expected_merge_tasks = sum(levels)
    assert len(merge_tasks) == expected_merge_tasks, merge_tasks

    total_sent = sum(t["states_sent"] for t in tasks)
    total_received = sum(t["states_received"] for t in tasks)
    merge_received = sum(t["states_received"] for t in merge_tasks)
    # Linear topology: every build task sends once, every non-root merge task forwards once, and
    # the root broadcasts once per probe task -- against N * N for all-to-all delivery.
    tree_edges = buckets + sum(levels[:-1])
    broadcast_edges = buckets
    # Bounded, not pinned. The root serializes its union once per probe task. It skips that work
    # when every probe task has already closed its receive branch, so a broadcast send can be
    # missing. Observed at `N = 32` on a machine oversubscribed 6x. The tree leg below pins the
    # shape exactly; this bound is what separates a linear topology from all-to-all, which would
    # send `N * N`.
    assert total_sent <= tree_edges + broadcast_edges, (total_sent, tree_edges, broadcast_edges)

    # The build leg is the one exact per-state count here, and it is what separates a tree from
    # all-to-all at the leaves: a tree has each build task serialize its partial once, for its one
    # merge parent, where all-to-all would serialize once per destination.
    # `BuildRuntimeFilterPartialTransform` reaches that serialize on its own input finishing, and
    # bails out early only when its *data* output is gone -- never on the state of the merge
    # parent -- so a merge task closing its inputs cannot suppress the count.
    build_tasks = [t for t in tasks if not t["task"].startswith("rf_merge_")]
    assert all(t["states_sent"] <= 1 for t in build_tasks), tasks
    assert sum(t["states_sent"] for t in build_tasks) == buckets, tasks

    # Tree-leg completeness, conditioned on the guard that makes it an invariant rather than a
    # race. A merge task publishes only once every input has arrived
    # (`MergeRuntimeFiltersTransform::finalize`), so a root that forwarded anything proves the
    # whole tree leg: each state it merged was itself published by a merge task that had consumed
    # all of its own inputs.
    #
    # Unconditionally it is a race, and losing it needs no cancellation. A probe task closes its
    # receive branch as soon as its own scan ends, and once every destination has gone,
    # `MergeRuntimeFiltersTransform::prepare` closes the merge task's inputs unread -- discarding a
    # partial that had already arrived. At `N = 32` a probe scans only `BIG_ROWS / N` rows while
    # the build side moves `N` blooms through two merge levels, so the probes normally finish
    # first: two CI workers running this concurrently on one runner split 34/34 and 33/34.
    root_published = any(t["states_sent"] == buckets for t in merge_tasks)
    if root_published:
        assert merge_received == tree_edges, (merge_received, tree_edges, tasks)

    # The root's broadcast to the probe tasks is best-effort by design: a probe task cancels its
    # receive branch once its data work is done (`RuntimeFilterReceiveBranches::finish`), and a
    # filter that arrives after the scan it would have narrowed has nothing left to serve. So on
    # that leg `received` may legitimately fall short of `sent` -- pinning them equal is what made
    # this test fail in CI. It can never exceed it, because delivery invents no states.
    assert total_received <= total_sent, (total_received, total_sent, tasks)

    # Every state put on an exchange is the settings-sized bloom. On the send side that is exact:
    # the counter is incremented where the state is serialized.
    total_bytes_sent = sum(t["bytes_sent"] for t in tasks)
    total_bytes_received = sum(t["bytes_received"] for t in tasks)
    assert total_bytes_sent >= total_sent * BLOOM_BYTES, tasks
    # Conservation, and every state that did arrive is one of those blooms.
    assert total_bytes_received <= total_bytes_sent, tasks
    assert total_bytes_received >= total_received * BLOOM_BYTES, tasks

    assert all(t["oversized_rejected"] == 0 for t in tasks), tasks

    # The fan-in bound: no receiving task consumes more states than the tree fan-in, whatever the
    # total task count -- the receiver-side buffering cannot scale with the cluster. A final
    # (probe) receiver consumes exactly one state, the complete global union, where all-to-all
    # delivery would hand it one state per build task.
    for task in tasks:
        assert task["states_received"] <= FAN_IN, task
        if not task["task"].startswith("rf_merge_"):
            assert task["states_received"] <= 1, task

    return merge_tasks, total_received - merge_received


def _assert_broadcast_delivered(probe_arrivals):
    """At least one probe task consumed the complete union somewhere in a bucket sweep. Takes a
    whole sweep and never a single topology: at one topology this is a race the receiver is
    allowed to lose, and losing it is not a defect. Measured on a machine oversubscribed 6x, 10 of
    139 single topologies saw no arrival, while none of 93 sweeps came up empty and the emptiest
    of them still delivered 2 states. Worth asserting even so: every other count here is taken
    where a state is serialized, so a broadcast that dropped all of them would pass unnoticed.

    Under thread sanitizer the sweep loses often enough to be unusable in CI, so it is skipped
    there. Measured over the runs that carry this assertion, each of the two tests calling it went
    6 passes to 1 failure under tsan and 32 for 32 everywhere else, and the failures arrive as an
    all-zero sweep. So tsan does not make arrival impossible - it makes it roughly a one-in-seven
    loss, which is more than a required check can carry when the thing it watches is best-effort
    by design. Skipping it does give up the configuration where a delivery race would show most
    readily; the send-side counts, which are exact, stay in force there."""
    if INITIATOR.is_built_with_thread_sanitizer():
        logging.info("skipping the broadcast-arrival check under tsan: %s", probe_arrivals)
        return
    assert max(probe_arrivals) >= 1, probe_arrivals


def test_topology_is_linear(started_cluster):
    """N = 2, 4, 8 symmetric topologies: identical results and exactly 2 * N filter streams."""
    expected_totals = {}
    probe_arrivals = []
    for buckets in (2, 4, 8):
        result, tasks = _run_and_collect(JOIN_QUERY, buckets)
        assert result == JOIN_EXPECTED

        # N <= 16 build tasks collapse into a single root merge task.
        _, probe_received = _check_topology(tasks, buckets, levels=[1])
        probe_arrivals.append(probe_received)
        expected_totals[buckets] = sum(t["states_sent"] for t in tasks)
        logging.info(
            "buckets=%d states=%d bytes=%d probe_arrivals=%d",
            buckets,
            expected_totals[buckets],
            sum(t["bytes_sent"] for t in tasks),
            probe_received,
        )

    # Linear growth: at most 2 * N streams, against N * N for all-to-all. Bounded rather than
    # pinned for the same reason as in `_check_topology` -- the root may skip a broadcast whose
    # destinations have all finished. `merge_received == tree_edges` there pins the tree leg of
    # each of these topologies exactly.
    assert all(sent <= 2 * buckets for buckets, sent in expected_totals.items()), expected_totals
    _assert_broadcast_delivered(probe_arrivals)


def test_multi_level_tree_and_memory_bound(started_cluster):
    """A 32-task build side needs two merge levels; the receive-side peak memory of a merge task
    stays bounded by the fan-in and the payload, not by the build task count."""
    max_buckets = 32
    merge_tasks_by_buckets = {}
    probe_arrivals = []
    for buckets, levels in ((2, [1]), (8, [1]), (max_buckets, [2, 1])):
        result, tasks = _run_and_collect(JOIN_QUERY, buckets)
        assert result == JOIN_EXPECTED
        merge_tasks_by_buckets[buckets], probe_received = _check_topology(
            tasks, buckets, levels=levels
        )
        probe_arrivals.append(probe_received)
        logging.info(
            "buckets=%d merge tasks=%s",
            buckets,
            [
                (t["task"], t["states_received"], t["memory_usage"])
                for t in merge_tasks_by_buckets[buckets]
            ],
        )

    # Receive side: the peak is bounded by the fan-in, not by the build task count -- `O(fan_in)`,
    # as `MergeRuntimeFiltersStep.h` puts it, because each streaming exchange input holds at most
    # one in-flight packet. So the slack has to cover the whole fan-in, and one in-flight state
    # costs two payloads, not one: it lands in a `ColumnString`, whose `PaddedPODArray` rounds the
    # `BLOOM_BYTES + 8` bytes up to the next power of two. Add one payload for the accumulator and
    # one for the re-serialized union. Compressed packet bodies are a few KiB here -- each state
    # holds at most 64000/32 keys in 2 MiB, so the array is over 99% zeroes.
    #
    # This does NOT test that the transform releases each state after merging: retention's floor
    # sits below the streaming ceiling, so no constant separates them, and under a sanitizer the
    # retained forms are invisible to the tracker anyway. That property belongs to
    # `MergeRuntimeFiltersTransform.PayloadRetentionIndependentOfInputCount`.
    fan_in_receivers = [
        t
        for t in merge_tasks_by_buckets[max_buckets]
        if t["states_received"] == FAN_IN
    ]
    # A first-level merge forwards once, and only after all of its inputs arrived, so one that
    # forwarded is necessarily a full-fan-in receiver. Requiring one unconditionally is the same
    # race as the tree-leg count above: if the probes finish first, the cascade closes these tasks'
    # inputs unread and none of them reaches the fan-in.
    first_level_published = [
        t
        for t in merge_tasks_by_buckets[max_buckets]
        if 0 < t["states_sent"] < max_buckets
    ]
    if first_level_published:
        assert fan_in_receivers, merge_tasks_by_buckets[max_buckets]
    two_input_root_peak = max(t["memory_usage"] for t in merge_tasks_by_buckets[2])
    for task in fan_in_receivers:
        assert task["memory_usage"] <= two_input_root_peak + (2 * FAN_IN + 2) * BLOOM_BYTES, (
            task,
            two_input_root_peak,
        )

    # The root's send side does not scale with the destination count: `CopyTransform` clones the
    # chunk per destination, and cloning shares the payload column, so the 32-destination root at
    # N = 32 peaks within a couple of KiB of the 2-destination root at N = 2. The bound below is
    # therefore far looser than what it watches; it still catches a regression that materialized
    # one copy per destination, which is what the replaced topology paid per build task.
    root_peak = max(t["memory_usage"] for t in merge_tasks_by_buckets[max_buckets])
    assert root_peak <= two_input_root_peak + (max_buckets + FAN_IN) * BLOOM_BYTES, (
        root_peak,
        two_input_root_peak,
    )

    _assert_broadcast_delivered(probe_arrivals)


def test_exact_states_topology(started_cluster):
    """With fixed-width keys and default limits the estimates keep the transported states exact
    (not bloom): the stream counts are identical, only the byte volume differs."""
    query_id = f"rf_tree_{uuid.uuid4().hex}"
    result = INITIATOR.query(
        f"SELECT count() FROM big, small WHERE toUInt64(bid) = toUInt64(sid) "
        f"SETTINGS {_dist_settings(4)}",
        query_id=query_id,
    ).strip()
    assert result == JOIN_EXPECTED

    tasks = _collect_task_rows(query_id)
    total_sent = sum(t["states_sent"] for t in tasks)
    total_received = sum(t["states_received"] for t in tasks)
    merge_received = sum(
        t["states_received"] for t in tasks if t["task"].startswith("rf_merge_")
    )
    # 4 into the root + 4 broadcast, bounded for the same reason as in `_check_topology`: the
    # root may skip a broadcast whose destinations have all finished.
    assert total_sent <= 8, tasks
    # The same delivery split as in `_check_topology`, and conditioned the same way: a root that
    # forwarded proves it consumed all four partials, because `finalize` publishes nothing until
    # every input arrived. Unconditionally the probes can close their receive branches first and
    # leave an arrived partial unread.
    if any(t["states_sent"] == 4 for t in tasks if t["task"].startswith("rf_merge_")):
        assert merge_received == 4, (merge_received, tasks)
    assert total_received <= total_sent, (total_received, total_sent, tasks)

    # The states must be exact, not degraded blooms: the default bloom is 512 KiB, and the 64000
    # exact keys of this join stay far below it, so a single degraded state would push some task's
    # byte counter past this bound. Without this the byte volume was only logged, so a regression
    # that transported blooms instead of exact states would have passed.
    default_bloom_bytes = 512 * 1024
    for task in tasks:
        assert task["bytes_received"] < default_bloom_bytes, task
        assert task["bytes_sent"] < 4 * default_bloom_bytes, task  # root: 4 union copies
    logging.info(
        "exact-state bytes=%d (vs %d for one default bloom)",
        sum(t["bytes_sent"] for t in tasks),
        default_bloom_bytes,
    )


def test_short_string_keys_arrive_exact(started_cluster):
    """20000 distinct short `String` keys under default limits. The estimate raises the
    transported row bound for variable-width keys as for fixed-width ones, so every
    transported state (partials, merged union, probe broadcast) stays exact, sized by
    actual key bytes, well below the settings bloom that any one degraded state would reach."""
    query_id = f"rf_tree_{uuid.uuid4().hex}"
    result = INITIATOR.query(
        f"SELECT count() FROM big INNER JOIN tiny ON bid = tid "
        f"SETTINGS {_dist_settings(4)}",
        query_id=query_id,
    ).strip()
    assert result == str(TINY_ROWS)

    tasks = _collect_task_rows(query_id)
    total_sent = sum(t["states_sent"] for t in tasks)
    total_received = sum(t["states_received"] for t in tasks)
    merge_received = sum(
        t["states_received"] for t in tasks if t["task"].startswith("rf_merge_")
    )
    # 4 partials into the root + 4 broadcast copies of the union; bounded because the root may
    # skip a broadcast whose destinations have all finished. The tree leg is pinned whenever the
    # root forwarded, which proves it consumed all four partials.
    assert total_sent <= 8, tasks
    if any(t["states_sent"] == 4 for t in tasks if t["task"].startswith("rf_merge_")):
        assert merge_received == 4, (merge_received, tasks)
    assert total_received <= total_sent, (total_received, total_sent, tasks)
    assert all(t["oversized_rejected"] == 0 for t in tasks), tasks

    # The settings bloom is 512 KiB; the exact states of all 20000 short keys together stay far
    # below it, so a single degraded state would push some task's byte counter past this bound.
    settings_bloom_bytes = 512 * 1024
    for task in tasks:
        assert task["bytes_received"] < settings_bloom_bytes, task
        assert task["bytes_sent"] < 4 * settings_bloom_bytes, task  # root: 4 union copies


def test_persisted_exchange_delivers_and_terminates(started_cluster):
    """The whole filter chain follows the forced-persisted kind; the scheduler runs the chain
    to completion in dependency order without deadlocking, and every partial still reaches the
    root, which merges them and hands the union to the broadcast."""
    result, tasks = _run_and_collect(
        JOIN_QUERY,
        4,
        extra_settings=BLOOM_STATE_SETTINGS
        + ", distributed_plan_force_exchange_kind = 'Persisted'",
    )
    assert result == JOIN_EXPECTED
    _check_topology(tasks, 4, levels=[1])


def test_result_equality_nested_joins(started_cluster):
    """Two joins, each with its own filter and its own merge tree."""
    query = """
        SELECT count() FROM big
        INNER JOIN small AS s1 ON big.bid = s1.sid
        INNER JOIN small AS s2 ON big.bid = concat(s2.sid, '')
    """
    expected = INITIATOR.query(query).strip()
    result, tasks = _run_and_collect(query, 4)
    assert result == expected
    assert sum(t["oversized_rejected"] for t in tasks) == 0

    # Each join must actually get its own merge tree, otherwise the result equality above holds
    # vacuously and the test would pass with no filter transported at all.
    trees = {t["task"].rsplit("_", 1)[0] for t in tasks if t["task"].startswith("rf_merge_")}
    assert len(trees) == 2, tasks

    # The build leg, pinned exactly, is what separates a tree from all-to-all here: each of the
    # two trees has its four build tasks serialize their partial once, for their one merge parent,
    # where all-to-all would serialize once per destination. Asserted per task and in total, so
    # neither a second serialization nor a missing one passes.
    build_tasks = [t for t in tasks if not t["task"].startswith("rf_merge_")]
    assert all(t["states_sent"] <= 1 for t in build_tasks), tasks
    assert sum(t["states_sent"] for t in build_tasks) == 2 * 4, tasks

    # The broadcast leg is deliberately not counted. How many stages a filter is delivered to is
    # the planner's to choose: here `bid = s1.sid` and `bid = concat(s2.sid, '')` make `s1.sid`
    # equi-joined with `big.bid`, so the second filter may also be applied to the first join's
    # build side, and a read path that finds that site delivers to four more tasks than one that
    # does not. Both are correct and the extra delivery prunes strictly more, so pinning a
    # destination count here would only encode one planner's choice.
    total_sent = sum(t["states_sent"] for t in tasks)
    total_received = sum(t["states_received"] for t in tasks)
    assert total_received <= total_sent, tasks


def test_early_close_stays_correct(started_cluster):
    """A query that closes its streams early (LIMIT) must not hang on the filter branch and must
    return correct rows whether or not the filter managed to arrive (fail-open)."""
    result, tasks = _run_and_collect(
        "SELECT bid FROM big INNER JOIN small ON bid = sid ORDER BY bid LIMIT 1", 4
    )
    assert result == "0"

    # The filter branch must have been wired, otherwise "does not hang on the filter branch" is
    # vacuous. Delivery itself is racy against the early close, so require the tree, not arrival.
    assert any(t["task"].startswith("rf_merge_") for t in tasks), tasks

    # No worker task may linger after the early close.
    _assert_no_lingering_tasks()


def test_cancellation_terminates_workers(started_cluster):
    """KILL QUERY on the initiator must terminate every worker task of the distributed plan."""
    query_id = f"rf_tree_cancel_{uuid.uuid4().hex}"

    INITIATOR.get_query_request(
        f"SELECT count() FROM big AS a INNER JOIN big AS b ON a.bid = b.bid "
        f"INNER JOIN small ON a.bid = sid "
        f"WHERE NOT ignore(sleepEachRow(0.01)) "
        f"SETTINGS {_dist_settings(4, BLOOM_STATE_SETTINGS)}, max_block_size = 256",
        query_id=query_id,
    )

    # Wait until a *filter merge* task exists, then kill the initiator query. Waiting for any
    # ordinary stage would let the kill land before the filter branch was ever running, so the
    # test would not exercise cancelling a merge task at all.
    deadline = time.time() + 60
    while time.time() < deadline:
        merge_tasks = sum(
            int(
                node.query(
                    "SELECT count() FROM system.processes WHERE query_id LIKE '%::rf_merge_%'"
                ).strip()
            )
            for node in NODES
        )
        if merge_tasks > 0:
            break
        time.sleep(0.3)
    else:
        pytest.fail("filter merge tasks never started")

    INITIATOR.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
    _assert_no_lingering_tasks()


def _assert_no_lingering_tasks():
    deadline = time.time() + 60
    while time.time() < deadline:
        lingering = []
        for node in NODES:
            running = node.query(
                "SELECT query_id FROM system.processes "
                "WHERE query_id LIKE '%::stage_%' OR query_id LIKE '%::rf_merge_%'"
            ).strip()
            if running:
                lingering.append((node.name, running))
        if not lingering:
            return
        time.sleep(0.5)
    pytest.fail(f"worker tasks still running: {lingering}")
