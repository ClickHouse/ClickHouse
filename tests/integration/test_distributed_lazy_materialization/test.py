import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster


pytestmark = pytest.mark.timeout(180)

cluster = ClickHouseCluster(__file__)
initiator = cluster.add_instance(
    "initiator",
    main_configs=["configs/cluster.xml"],
    stay_alive=True,
)
shard1 = cluster.add_instance("shard1", stay_alive=True)
shard2 = cluster.add_instance("shard2", stay_alive=True)
shards = [shard1, shard2]

BASE_SETTINGS = ", ".join(
    [
        "serialize_query_plan = 1",
        "query_plan_optimize_lazy_materialization = 1",
        "distributed_push_down_limit = 1",
        "skip_unavailable_shards = 0",
        "allow_experimental_parallel_reading_from_replicas = 0",
        "max_parallel_replicas = 1",
        "log_queries = 1",
    ]
)
SELECT_QUERY = (
    "SELECT key, payload FROM distributed_data "
    "ORDER BY key DESC LIMIT 3 OFFSET 1"
)
EXPECTED = "90\ts2-90\n80\ts1-80\n70\ts2-70\n"
PAUSE_FAILPOINT = "distributed_top_k_pause_before_candidate_submission"
FALLBACK_FAILPOINT = "distributed_top_k_force_fallback"
THROW_FAILPOINT = "distributed_top_k_throw_after_candidate_selection"
COMPETING_QUERY = (
    "SELECT key, payload FROM competing_data "
    "ORDER BY key DESC LIMIT 3 OFFSET 1"
)


def _competing_settings(receive_data_timeout_ms):
    # `in_order` tries `shard2` first for the first shard; `shard1` is its hedged alternative,
    # started once `shard2` sends nothing for `receive_data_timeout_ms`.
    return ", ".join(
        [
            BASE_SETTINGS,
            "use_hedged_requests = 1",
            "load_balancing = 'in_order'",
            f"receive_data_timeout_ms = {receive_data_timeout_ms}",
            "query_plan_optimize_distributed_lazy_materialization = 1",
        ]
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for shard in shards:
            shard.query(
                "CREATE TABLE local_data (key UInt64, payload String) "
                "ENGINE = MergeTree ORDER BY tuple()"
            )
        shard1.query(
            "INSERT INTO local_data VALUES "
            "(100, 's1-100'), (80, 's1-80'), (60, 's1-60')"
        )
        _load_shard2()
        initiator.query(
            "CREATE TABLE distributed_data (key UInt64, payload String) "
            "ENGINE = Distributed(distributed_lazy_materialization, default, local_data, rand())"
        )
        _create_competing_replicas()
        yield cluster
    finally:
        cluster.shutdown()


def _create_competing_replicas():
    for shard in shards:
        shard.query("CREATE DATABASE competing_a")
        shard.query(
            "CREATE TABLE competing_a.competing_data (key UInt64, payload String) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )
        shard.query(
            "INSERT INTO competing_a.competing_data VALUES "
            f"(95, 'a-{shard.name}-95'), (85, 'a-{shard.name}-85'), (65, 'a-{shard.name}-65')"
        )
    shard1.query("CREATE DATABASE competing_b")
    shard1.query(
        "CREATE TABLE competing_b.competing_data (key UInt64, payload String) "
        "ENGINE = MergeTree ORDER BY tuple()"
    )
    shard1.query(
        "INSERT INTO competing_b.competing_data VALUES "
        "(90, 'b-90'), (80, 'b-80'), (70, 'b-70')"
    )
    initiator.query(
        "CREATE TABLE competing_data (key UInt64, payload String) "
        "ENGINE = Distributed(competing_replicas, '', competing_data, rand())"
    )


def _load_shard2():
    shard2.query("TRUNCATE TABLE local_data")
    shard2.query(
        "INSERT INTO local_data VALUES "
        "(90, 's2-90'), (70, 's2-70'), (50, 's2-50')"
    )


def _settings(enabled=True):
    if enabled:
        return f"{BASE_SETTINGS}, query_plan_optimize_distributed_lazy_materialization = 1"
    # Without the optimization, the classic path is used. With `serialize_query_plan = 1` it plans the source
    # table on the initiator, which has no `local_data`, so the baseline sends the query text instead.
    baseline = BASE_SETTINGS.replace("serialize_query_plan = 1", "serialize_query_plan = 0")
    return f"{baseline}, query_plan_optimize_distributed_lazy_materialization = 0"


def _run(query_id, enabled=True):
    return initiator.query(
        f"{SELECT_QUERY} SETTINGS {_settings(enabled)}",
        query_id=query_id,
    )


def _profile_event(node, query_id, event, remote=False):
    node.query("SYSTEM FLUSH LOGS")
    query_filter = f"query_id = '{query_id}'"
    if remote:
        query_filter = f"(initial_query_id = '{query_id}' OR query_id = '{query_id}')"
    value = node.query(
        f"SELECT sum(ProfileEvents[{event!r}]) FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND {query_filter}"
    )
    return int(value.strip())


def _remote_profile_event(query_id, event):
    return sum(_profile_event(shard, query_id, event, remote=True) for shard in shards)


def _assert_equivalent(enabled_query_id, expected=EXPECTED):
    disabled = _run(f"{enabled_query_id}-disabled", enabled=False)
    enabled = _run(enabled_query_id, enabled=True)
    assert disabled == enabled == expected
    return enabled


def test_winner_only_payload_materialization():
    query_id = f"distributed-lazy-selected-{uuid.uuid4()}"
    _assert_equivalent(query_id)

    assert _profile_event(initiator, query_id, "DistributedTopKCandidateRows") == 6
    assert _profile_event(initiator, query_id, "DistributedTopKCandidateBytes") > 0
    assert _profile_event(initiator, query_id, "DistributedTopKSelectedRows") == 4
    assert _profile_event(initiator, query_id, "DistributedTopKFallbacks") == 0

    # The candidate limit is four. Only the four global winners enter the payload read.
    assert _remote_profile_event(query_id, "LazyMaterializationRows") == 4


def test_empty_shard_participates():
    shard2.query("TRUNCATE TABLE local_data")
    try:
        query_id = f"distributed-lazy-empty-{uuid.uuid4()}"
        _assert_equivalent(query_id, "80\ts1-80\n60\ts1-60\n")

        assert _profile_event(initiator, query_id, "DistributedTopKCandidateRows") == 3
        assert _profile_event(initiator, query_id, "DistributedTopKSelectedRows") == 3
        assert _profile_event(initiator, query_id, "DistributedTopKFallbacks") == 0
        assert _remote_profile_event(query_id, "LazyMaterializationRows") == 3
    finally:
        _load_shard2()


def test_fallback_materializes_all_local_candidates():
    shard2.query(f"SYSTEM ENABLE FAILPOINT {FALLBACK_FAILPOINT}")
    query_id = f"distributed-lazy-fallback-{uuid.uuid4()}"
    try:
        _assert_equivalent(query_id)
    finally:
        shard2.query(f"SYSTEM DISABLE FAILPOINT {FALLBACK_FAILPOINT}")

    assert _profile_event(initiator, query_id, "DistributedTopKCandidateRows") == 3
    assert _profile_event(initiator, query_id, "DistributedTopKSelectedRows") == 0
    assert _profile_event(initiator, query_id, "DistributedTopKFallbacks") == 1
    assert _profile_event(initiator, query_id, "DistributedTopKFallbackShards") == 2
    assert _remote_profile_event(query_id, "DistributedTopKFallbackRows") == 6
    assert _remote_profile_event(query_id, "LazyMaterializationRows") == 6


def test_cancellation_wakes_waiting_candidate():
    shard2.query(f"SYSTEM ENABLE FAILPOINT {PAUSE_FAILPOINT}")
    query_id = f"distributed-lazy-cancel-{uuid.uuid4()}"
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(
            lambda: initiator.query_and_get_error(
                f"{SELECT_QUERY} SETTINGS {_settings()}",
                query_id=query_id,
                timeout=30,
            )
        )
        shard2.query(f"SYSTEM WAIT FAILPOINT {PAUSE_FAILPOINT} PAUSE")

        waiting_events = initiator.query_with_retry(
            "SELECT tuple("
            "ProfileEvents['DistributedTopKCandidateRows'], "
            "ProfileEvents['DistributedTopKSelectedRows']) "
            f"FROM system.processes WHERE query_id = '{query_id}'",
            retry_count=30,
            sleep_time=0.1,
            check_callback=lambda value: value.startswith("(3,0)"),
        )
        assert waiting_events.startswith("(3,0)")

        initiator.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
        error = future.result(timeout=15)
        assert "cancel" in error.lower()
    finally:
        shard2.query(f"SYSTEM NOTIFY FAILPOINT {PAUSE_FAILPOINT}")
        shard2.query(f"SYSTEM DISABLE FAILPOINT {PAUSE_FAILPOINT}")
        executor.shutdown(wait=True)

    assert _run(f"{query_id}-after-cancel") == EXPECTED


def test_competing_replicas_commit_first_candidate_set():
    # `shard2` stalls before submitting its candidates, so the hedged request starts `shard1`
    # for the same shard. Only the first complete candidate set may commit: every row of
    # that shard must come from `shard1`, and the cancelled `shard2` must contribute nothing.
    shard2.query(f"SYSTEM ENABLE FAILPOINT {PAUSE_FAILPOINT}")
    query_id = f"distributed-lazy-competing-{uuid.uuid4()}"
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(
            lambda: initiator.query(
                f"{COMPETING_QUERY} SETTINGS {_competing_settings(500)}",
                query_id=query_id,
                timeout=60,
            )
        )
        shard2.query(f"SYSTEM WAIT FAILPOINT {PAUSE_FAILPOINT} PAUSE")
        result = future.result(timeout=60)
    finally:
        shard2.query(f"SYSTEM NOTIFY FAILPOINT {PAUSE_FAILPOINT}")
        shard2.query(f"SYSTEM DISABLE FAILPOINT {PAUSE_FAILPOINT}")
        executor.shutdown(wait=True)

    assert result == "90\tb-90\n85\ta-shard1-85\n80\tb-80\n"
    assert _profile_event(initiator, query_id, "HedgedRequestsChangeReplica") >= 1
    # Three candidates from each shard; the loser's candidates are never merged.
    assert _profile_event(initiator, query_id, "DistributedTopKCandidateRows") == 6


def test_competing_replicas_committed_replica_failure_fails_query():
    # `shard2` submits its candidates first and is committed, then fails. The query must fail
    # instead of switching to `shard1` or returning the rows of the other shard alone.
    shard2.query(f"SYSTEM ENABLE FAILPOINT {THROW_FAILPOINT}")
    query_id = f"distributed-lazy-committed-failure-{uuid.uuid4()}"
    try:
        error = initiator.query_and_get_error(
            f"{COMPETING_QUERY} SETTINGS {_competing_settings(30000)}",
            query_id=query_id,
            timeout=60,
        )
    finally:
        shard2.query(f"SYSTEM DISABLE FAILPOINT {THROW_FAILPOINT}")

    assert "Injected failure after distributed TopK candidate selection" in error

    assert (
        initiator.query(f"{COMPETING_QUERY} SETTINGS {_competing_settings(30000)}")
        == "90\tb-90\n85\ta-shard2-85\n80\tb-80\n"
    )
