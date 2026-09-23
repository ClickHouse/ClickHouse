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
        # A one-row table holding the dictionary name, for the scalar-subquery form of `assignCentroid`. Replicated: the
        # scalar subquery is a unit of its own and has no dictionary, so it distributes and reads the table on the worker.
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE cfg (nm String)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/cfg', '{replica}') ORDER BY nm
                """
            )
        initiator.query("INSERT INTO cfg SELECT 'default.c'")
        worker.query("SYSTEM SYNC REPLICA cfg")
        # A `Join` table on the initiator only: `joinGet` resolves it by name on the executing server, like a dictionary.
        initiator.query("CREATE TABLE jt (k UInt64, name String) ENGINE = Join(ANY, LEFT, k)")
        initiator.query("INSERT INTO jt SELECT number, concat('j', toString(number)) FROM numbers(1000)")
        assert worker.query("SELECT count() FROM system.tables WHERE name = 'jt'").strip() == "0"
        # A table whose `DEFAULT` / `MATERIALIZED` columns were added after the insert, so the only part lacks them and a
        # reader has to compute them from the metadata. Separate from `t`: the `count()` controls pick the smallest column.
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE t_dflt (k UInt64, v String)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t_dflt', '{replica}') ORDER BY k
                """
            )
        initiator.query("INSERT INTO t_dflt SELECT number, toString(number) FROM numbers(1000)")
        initiator.query(
            "ALTER TABLE t_dflt ADD COLUMN nm String DEFAULT dictGet(d, 'name', k), "
            "ADD COLUMN mt String MATERIALIZED dictGet(d, 'name', k), "
            "ADD COLUMN via String DEFAULT concat(nm, '!'), "
            "ADD COLUMN via2 String DEFAULT upper(via), "
            "ADD COLUMN lam Array(String) DEFAULT arrayMap(x -> dictGet(d, 'name', x), [k]), "
            "ADD COLUMN cid UInt32 DEFAULT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], 'default.c'), "
            "ADD COLUMN cid_inline UInt32 DEFAULT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], "
            "[[1.0, 0.0], [0.0, 1.0]]::Array(Array(Float32))), "
            "ADD COLUMN cid_inline_literal UInt32 DEFAULT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], [[1.0, 0.0], [0.0, 1.0]]), "
            "ADD COLUMN plain String DEFAULT concat('p', toString(k))"
        )
        worker.query("SYSTEM SYNC REPLICA t_dflt")
        # An ALIAS column with a dictionary call, on a table of its own: the analyzer resolves alias expressions of a table
        # when it initializes the table expression, whichever columns the query uses, so the record sees the dictionary
        # for every query over this table.
        # Added by ALTER on the initiator: a CREATE with the alias would validate the expression on the worker, which has
        # no dictionary; the replicated metadata change is not validated there.
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE t_alias (k UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t_alias', '{replica}') ORDER BY k
                """
            )
        initiator.query("INSERT INTO t_alias SELECT number FROM numbers(100)")
        initiator.query("ALTER TABLE t_alias ADD COLUMN al String ALIAS dictGet(d, 'name', k)")
        worker.query("SYSTEM SYNC REPLICA t_alias")
        # A materialized default whose dictionary exists on both nodes (each replica runs the mutation itself): every part
        # holds the column, so no reader would evaluate the default, yet the check does not look at parts.
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE t_mat (k UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t_mat', '{replica}') ORDER BY k
                """
            )
            node.query("CREATE TABLE src_both (k UInt64, name String) ENGINE = MergeTree ORDER BY k")
            node.query("INSERT INTO src_both SELECT number, concat('n', toString(number)) FROM numbers(100)")
            node.query(
                """
                CREATE DICTIONARY d_both (k UInt64, name String) PRIMARY KEY k
                SOURCE(CLICKHOUSE(TABLE 'src_both' DB 'default')) LAYOUT(FLAT()) LIFETIME(0)
                """
            )
        initiator.query("INSERT INTO t_mat SELECT number FROM numbers(100)")
        initiator.query("ALTER TABLE t_mat ADD COLUMN m String DEFAULT dictGet(d_both, 'name', k)")
        initiator.query("ALTER TABLE t_mat MATERIALIZE COLUMN m SETTINGS mutations_sync = 2")
        worker.query("SYSTEM SYNC REPLICA t_mat")
        # A materialized default whose dictionary was dropped afterwards: the parts hold the values, but the default
        # expression in the metadata no longer resolves. The dictionary has to exist on both replicas while the
        # materialization runs (each replica executes the mutation itself), and is dropped on both afterwards.
        for node in (initiator, worker):
            node.query(
                """
                CREATE TABLE t_gone (k UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t_gone', '{replica}') ORDER BY k
                """
            )
            node.query("CREATE TABLE src_gone (k UInt64, name String) ENGINE = MergeTree ORDER BY k")
            node.query("INSERT INTO src_gone SELECT number, concat('n', toString(number)) FROM numbers(100)")
            node.query(
                """
                CREATE DICTIONARY d_gone (k UInt64, name String) PRIMARY KEY k
                SOURCE(CLICKHOUSE(TABLE 'src_gone' DB 'default')) LAYOUT(FLAT()) LIFETIME(0)
                """
            )
        initiator.query("INSERT INTO t_gone SELECT number FROM numbers(100)")
        initiator.query("ALTER TABLE t_gone ADD COLUMN gone String DEFAULT dictGet(d_gone, 'name', k)")
        initiator.query("ALTER TABLE t_gone MATERIALIZE COLUMN gone SETTINGS mutations_sync = 2")
        worker.query("SYSTEM SYNC REPLICA t_gone")
        # The dependency tracking refuses the drop (the default of `gone` uses the dictionary); forced here on purpose.
        for node in (initiator, worker):
            node.query("DROP DICTIONARY d_gone SETTINGS check_table_dependencies = 0")
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
    assert "does not support dictionary default.d" in _fallback_reasons(query_id)


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
    assert "does not support dictionary default.d" in _fallback_reasons(query_id)


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
    assert "does not support dictionary default.d" in _fallback_reasons(query_id)

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
    assert "does not support dictionary default.d" in _fallback_reasons(query_id)


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
    assert "does not support dictionary default.c" in _fallback_reasons(query_id)

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


def test_assign_centroid_wrapped_name_falls_back(started_cluster):
    """The function sees the dictionary name unwrapped, but the plan node keeps the type as written: `Nullable(String)`
    from a scalar subquery, `LowCardinality(String)` from `toLowCardinality`. Issue 121486."""
    # The scalar subquery is a unit of its own: it has no dictionary, so it distributes and spawns tasks of its own while
    # the outer plan falls back. Only the outer plan's decision is asserted for it.
    for name_expression, outer_plan_only in [
        ("toLowCardinality('default.c')", False),
        ("CAST('default.c', 'Nullable(String)')", False),
        ("(SELECT nm FROM cfg LIMIT 1)", True),
    ]:
        query_id = str(uuid.uuid4())
        result = initiator.query(
            f"SELECT assignCentroid([toFloat32(k % 2), toFloat32(1 - k % 2)], {name_expression}) AS cid, count() "
            f"FROM t GROUP BY cid ORDER BY cid SETTINGS {DISTRIBUTED_SETTINGS}",
            query_id=query_id,
        )
        assert result == "0\t500\n1\t500\n", name_expression
        _flush_logs()
        if not outer_plan_only:
            assert _remote_tasks(query_id) == 0, name_expression
            assert _worker_tasks(query_id) == 0, name_expression
        assert "does not support dictionary default.c" in _fallback_reasons(query_id), name_expression


def test_join_get_falls_back(started_cluster):
    """`joinGet` resolves its `Join` table in the catalog of the executing server when the function is built, exactly
    like a dictionary function does. Issue 121487."""
    for query, expected in [
        ("SELECT k, joinGet(jt, 'name', k) AS name FROM t ORDER BY k LIMIT 3", "0\tj0\n1\tj1\n2\tj2\n"),
        ("SELECT k, joinGetOrNull('default.jt', 'name', k) AS name FROM t ORDER BY k LIMIT 3", "0\tj0\n1\tj1\n2\tj2\n"),
        ("SELECT count() FROM t WHERE joinGet(jt, 'name', k) = 'j5'", "1\n"),
        ("SELECT k, arrayMap(x -> joinGet(jt, 'name', x), [k]) AS names FROM t ORDER BY k LIMIT 2", "0\t['j0']\n1\t['j1']\n"),
    ]:
        query_id = str(uuid.uuid4())
        result = initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id)
        assert result == expected, query
        _flush_logs()
        assert _remote_tasks(query_id) == 0, query
        assert _worker_tasks(query_id) == 0, query
        assert "does not support Join table default.jt" in _fallback_reasons(query_id), query


def test_dict_get_in_column_default_falls_back(started_cluster):
    """The plan carries only `INPUT nm`; the worker's reader would compute the default from the metadata, on the worker,
    with a dictionary it does not have. A default reading another defaulted column is followed. Issue 121489."""
    # `via` reads `nm`, `via2` reads `via`: the reader would compute the whole chain, so the check follows it. `lam` hides
    # the call in a lambda body. `cid` is the dictionary form of `assignCentroid` spelled in a default.
    # The reason names the function and the selected column whose resolved default contains it: for `via` and `via2` the
    # analyzer substitutes `nm` by its own default, so the call is found in their expressions.
    for query, expected, reason in [
        ("SELECT k, nm FROM t_dflt ORDER BY k LIMIT 3", "0\tn0\n1\tn1\n2\tn2\n", "dictionary default.d"),
        ("SELECT k, mt FROM t_dflt ORDER BY k LIMIT 3", "0\tn0\n1\tn1\n2\tn2\n", "dictionary default.d"),
        ("SELECT k, via FROM t_dflt ORDER BY k LIMIT 3", "0\tn0!\n1\tn1!\n2\tn2!\n", "dictionary default.d"),
        ("SELECT k, via2 FROM t_dflt ORDER BY k LIMIT 3", "0\tN0!\n1\tN1!\n2\tN2!\n", "dictionary default.d"),
        ("SELECT k, lam FROM t_dflt ORDER BY k LIMIT 2", "0\t['n0']\n1\t['n1']\n", "dictionary default.d"),
        ("SELECT k, cid FROM t_dflt ORDER BY k LIMIT 3", "0\t1\n1\t0\n2\t1\n", "dictionary default.c"),
    ]:
        query_id = str(uuid.uuid4())
        result = initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id)
        assert result == expected, query
        _flush_logs()
        assert _remote_tasks(query_id) == 0, query
        assert _worker_tasks(query_id) == 0, query
        assert f"does not support {reason}: it is an object of the initiator, used by a column default of table default.t_dflt" in _fallback_reasons(query_id), query

    # Defaults without an object of the initiator still distribute: a plain expression, and the inline form of `assignCentroid`.
    for query, expected in [
        ("SELECT k, plain FROM t_dflt ORDER BY k LIMIT 2", "0\tp0\n1\tp1\n"),
        ("SELECT k, cid_inline FROM t_dflt ORDER BY k LIMIT 3", "0\t1\n1\t0\n2\t1\n"),
        ("SELECT k, cid_inline_literal FROM t_dflt ORDER BY k LIMIT 3", "0\t1\n1\t0\n2\t1\n"),
    ]:
        query_id = str(uuid.uuid4())
        result = initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id)
        assert result == expected, query
        _flush_logs()
        assert _remote_tasks(query_id) > 0, query
        assert _fallback_reasons(query_id) == "", query


def test_column_default_check_boundaries(started_cluster):
    """What the column-default check does and does not look at: only the columns the read produces (a query without a
    defaulted column distributes; a virtual column is skipped), never the parts (a materialized default still falls back,
    the accepted imprecision of the temporary check), and not aliases (the analyzer inlines them into the query)."""
    for query, expected in [
        ("SELECT k FROM t_dflt ORDER BY k LIMIT 2", "0\n1\n"),
        ("SELECT k, _part != '' FROM t_dflt ORDER BY k LIMIT 2", "0\t1\n1\t1\n"),
    ]:
        query_id = str(uuid.uuid4())
        assert initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id) == expected, query
        _flush_logs()
        assert _remote_tasks(query_id) > 0, query
        assert _fallback_reasons(query_id) == "", query

    query_id = str(uuid.uuid4())
    assert initiator.query(f"SELECT k, m FROM t_mat ORDER BY k LIMIT 2 SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id) == "0\tn0\n1\tn1\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert "does not support dictionary default.d_both: it is an object of the initiator, used by a column default of table default.t_mat" in _fallback_reasons(query_id)

    # An ALIAS is inlined by the analyzer, so it is caught through the query text, not through the column-default check;
    # and because the analyzer resolves the alias expressions of a table up front, a query that does not use the alias
    # falls back as well. Accepted while dictionaries are disabled for distributed plans.
    for query, expected in [
        ("SELECT k, al FROM t_alias ORDER BY k LIMIT 2", "0\tn0\n1\tn1\n"),
        ("SELECT k FROM t_alias ORDER BY k LIMIT 2", "0\n1\n"),
    ]:
        query_id = str(uuid.uuid4())
        assert initiator.query(f"{query} SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id) == expected, query
        _flush_logs()
        assert _remote_tasks(query_id) == 0, query
        reasons = _fallback_reasons(query_id)
        assert "does not support dictionary default.d: it is an object of the initiator" in reasons, query
        assert "column default" not in reasons, query


def test_unresolvable_column_default_falls_back(started_cluster):
    """The default expression of `gone` names a dictionary that no longer exists. The check cannot resolve it and treats
    that as a reference, so the query runs locally, where the reader finds the column in every part and never evaluates
    the default. Distributing would have been fine here; failing at planning would not."""
    query_id = str(uuid.uuid4())
    result = initiator.query(f"SELECT k, gone FROM t_gone ORDER BY k LIMIT 2 SETTINGS {DISTRIBUTED_SETTINGS}", query_id=query_id)
    assert result == "0\tn0\n1\tn1\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "a column default of table default.t_gone does not resolve" in _fallback_reasons(query_id)


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
        assert "does not support dictionary default.d" in _fallback_reasons(query_id), query


def test_scalar_subquery_with_dictionary_disables_the_query(started_cluster):
    """The record is per query: a dictionary resolved while a scalar subquery is evaluated on the initiator makes the
    outer plan fall back too, although only its constant result would have shipped. Accepted while dictionaries are
    disabled for distributed plans."""
    query_id = str(uuid.uuid4())
    result = initiator.query(
        f"SELECT k, (SELECT dictGet(d, 'name', toUInt64(1))) AS s FROM t ORDER BY k LIMIT 2 SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )
    assert result == "0\tn1\n1\tn1\n"
    _flush_logs()
    assert _remote_tasks(query_id) == 0
    assert _worker_tasks(query_id) == 0
    assert "does not support dictionary default.d" in _fallback_reasons(query_id)


def test_record_does_not_depend_on_query_logging(started_cluster):
    """`system.query_log` bookkeeping is gated on `log_queries`; the record the decision reads is not."""
    error = initiator.query_and_get_error(
        f"SELECT k, dictGet(d, 'name', k) FROM t ORDER BY k LIMIT 3 "
        f"SETTINGS {DISTRIBUTED_SETTINGS}, distributed_plan_fallback_to_local_execution = 0, log_queries = 0"
    )
    assert "SUPPORT_IS_DISABLED" in error
    assert "does not support dictionary default.d" in error


def test_strict_mode_throws(started_cluster):
    error = initiator.query_and_get_error(
        f"SELECT k, dictGet(d, 'name', k) FROM t ORDER BY k LIMIT 3 "
        f"SETTINGS {DISTRIBUTED_SETTINGS}, distributed_plan_fallback_to_local_execution = 0"
    )
    assert "SUPPORT_IS_DISABLED" in error
    assert "does not support dictionary default.d" in error
