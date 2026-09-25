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
        # The `assignCentroid` cases aggregate; a global GROUP BY limit would be a fallback reason of its own.
        "max_rows_to_group_by = 0",
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
        # A centroid dictionary for `assignCentroid`, again on the initiator only.
        initiator.query("CREATE TABLE centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid")
        initiator.query("INSERT INTO centroids SELECT number, [toFloat32(1 - number), toFloat32(number)] FROM numbers(2)")
        initiator.query(
            """
            CREATE DICTIONARY c (cid UInt64, vec Array(Float32)) PRIMARY KEY cid
            SOURCE(CLICKHOUSE(TABLE 'centroids' DB 'default')) LAYOUT(HASHED()) LIFETIME(0)
            """
        )
        initiator.query("SYSTEM RELOAD DICTIONARY c")
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


def test_dict_get_inside_lambda_falls_back(started_cluster):
    """A lambda body is a DAG of its own, so the check has to descend into it; a lambda that captures nothing is
    folded into a constant column and reachable only through that column."""
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT k, arrayMap(x -> dictGet(d, 'name', x), [k]) AS names FROM t ORDER BY k LIMIT 2 SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "0\t['n0']\n1\t['n1']\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support the dictionary function dictGet" in _fallback_reasons(query_id)

    # The lambda captures `k`, so it stays a `FunctionCapture` node whose body holds the call.
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT count() FROM t WHERE arrayExists(x -> dictHas(d, x + k), [0]) AND k < 10 SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "10\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support the dictionary function dictHas" in _fallback_reasons(query_id)


def test_assign_centroid_dictionary_form_falls_back(started_cluster):
    """`assignCentroid` reaches a dictionary only through its `String` argument; the plan holds just that constant,
    so the function is recognised by name and argument type. The inline form carries its centroids and stays distributed."""
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], 'default.c') AS cid, count() FROM t GROUP BY cid ORDER BY cid "
        f"SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "0\t500\n1\t500\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support the dictionary function assignCentroid" in _fallback_reasons(query_id)

    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], [[1.0, 0.0], [0.0, 1.0]]::Array(Array(Float32))) AS cid, count() "
        f"FROM t GROUP BY cid ORDER BY cid SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "0\t500\n1\t500\n"
    _flush_logs()
    assert _remote_tasks(query_id) > 0
    assert _fallback_reasons(query_id) == ""


def test_dict_in_limit_range_and_interpolate_falls_back(started_cluster):
    """Steps other than expression and filter carry a DAG too: the `LIMIT AFTER` / `UNTIL` boundaries sit in the
    `LimitRange` step above the gather, and `INTERPOLATE` in the `Filling` step."""
    for query, function in [
        ("SELECT k FROM t ORDER BY k LIMIT AFTER dictHas(d, 1996 - k)", "dictHas"),
        ("SELECT k FROM t ORDER BY k LIMIT UNTIL NOT dictHas(d, k + 997)", "dictHas"),
        ("SELECT k, v FROM t WHERE k < 2 ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (v AS dictGet(d, 'name', k))", "dictGet"),
    ]:
        query_id = str(uuid.uuid4())
        result = initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id)
        assert result != ""
        _flush_logs()
        assert _remote_tasks(query_id) == 0, query
        assert _worker_tasks(query_id) == 0, query
        assert f"does not support the dictionary function {function}" in _fallback_reasons(query_id), query


def test_strict_mode_throws(started_cluster):
    error = initiator.query_and_get_error(
        f"SELECT k, dictGet(d, 'name', k) FROM t ORDER BY k LIMIT 3 "
        f"SETTINGS {DISTRIBUTED_SETTINGS}, distributed_plan_fallback_to_local_execution = 0"
    )
    assert "SUPPORT_IS_DISABLED" in error
    assert "does not support the dictionary function dictGet" in error
