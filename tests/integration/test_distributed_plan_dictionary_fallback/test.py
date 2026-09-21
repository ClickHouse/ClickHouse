"""
The distributed plan ships the expression to the workers as a name: the fragment carries `dictGet('db.dict', ...)`, and the
worker task rebuilds the function and resolves `db.dict` in its own catalog. Nothing ships the dictionary itself yet, so on a
worker that does not have it the task fails with `Dictionary (...) not found` while executing `ReadFromDistributedPlanSource`,
and the whole query fails, hence the fallback to local execution.
This cluster puts every worker task on a second node that has the table but not the dictionary to test the fallback.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

pytestmark = pytest.mark.timeout(300)

cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance(
    "initiator",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
worker = cluster.add_instance(
    "worker",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)

DISTRIBUTED_SETTINGS = ", ".join(
    [
        "make_distributed_plan = 1",
        "enable_parallel_replicas = 0",
        "automatic_parallel_replicas_mode = 0",
        "distributed_plan_default_reader_bucket_count = 2",
        # The runner may randomize it; the rewrite into `IN (SELECT ... FROM dictionary)` takes the function out of the plan.
        "optimize_inverse_dictionary_lookup = 0",
    ]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE t (k UInt64, v String)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') ORDER BY k
                """
            )
        initiator.query("INSERT INTO t SELECT number, toString(number) FROM numbers(1000)")
        worker.query("SYSTEM SYNC REPLICA t")
        # The dictionary exists on the initiator only (Atomic database, not replicated).
        initiator.query("CREATE TABLE src (k UInt64, name String) ENGINE = MergeTree ORDER BY k")
        initiator.query("INSERT INTO src SELECT number, concat('n', toString(number)) FROM numbers(1000)")
        initiator.query(
            """
            CREATE DICTIONARY d (k UInt64, name String) PRIMARY KEY k
            SOURCE(CLICKHOUSE(TABLE 'src' DB 'default')) LAYOUT(FLAT()) LIFETIME(0)
            """
        )
        initiator.query("SYSTEM RELOAD DICTIONARY d")
        assert worker.query("SELECT count() FROM system.dictionaries WHERE name = 'd'").strip() == "0"
        yield cluster
    finally:
        cluster.shutdown()


def _flush_logs():
    for node in (initiator, worker):
        node.query("SYSTEM FLUSH LOGS query_log, text_log")


def _remote_tasks(query_id: str) -> int:
    return int(
        initiator.query(
            f"SELECT ProfileEvents['DistributedPlanRemoteTasks'] FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'"
        ).strip()
    )


def _worker_tasks(query_id: str) -> int:
    return int(
        worker.query(
            f"SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND initial_query_id = '{query_id}'"
        ).strip()
    )


def _fallback_reasons(query_id: str) -> str:
    return initiator.query(
        f"SELECT message FROM system.text_log WHERE query_id = '{query_id}' "
        f"AND logger_name = 'makeDistributedPlan' AND message LIKE '%falling back to local execution%'"
    )


def test_dict_get_falls_back(started_cluster):
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT k, dictGet(d, 'name', k) AS name FROM t ORDER BY k LIMIT 3 SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "0\tn0\n1\tn1\n2\tn2\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support the dictionary function dictGet" in _fallback_reasons(query_id)


def test_dict_get_in_filter_falls_back(started_cluster):
    """The filter may be moved into the prewhere of the read; the function is found there as well."""
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT count() FROM t WHERE dictHas(d, k) AND k < 10 SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "10\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support the dictionary function dictHas" in _fallback_reasons(query_id)


def test_strict_mode_throws(started_cluster):
    error = initiator.query_and_get_error(
        f"SELECT k, dictGet(d, 'name', k) FROM t ORDER BY k LIMIT 3 "
        f"SETTINGS {DISTRIBUTED_SETTINGS}, distributed_plan_fallback_to_local_execution = 0"
    )
    assert "SUPPORT_IS_DISABLED" in error
    assert "does not support the dictionary function dictGet" in error
