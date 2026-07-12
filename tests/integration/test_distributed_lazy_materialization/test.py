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
        "enable_analyzer = 1",
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
        yield cluster
    finally:
        cluster.shutdown()


def _load_shard2():
    shard2.query("TRUNCATE TABLE local_data")
    shard2.query(
        "INSERT INTO local_data VALUES "
        "(90, 's2-90'), (70, 's2-70'), (50, 's2-50')"
    )


def _settings(enabled=True):
    value = 1 if enabled else 0
    return f"{BASE_SETTINGS}, query_plan_optimize_distributed_lazy_materialization = {value}"


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
