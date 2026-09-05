-- Tags: replica

DROP TABLE IF EXISTS t_window_const;
CREATE TABLE t_window_const (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_const SELECT toString(number) FROM numbers(2000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

-- A window function that does not reference the constant column projected by the inner subquery.
-- Each query below must return the row count; a header that drops the unused constant makes the
-- replica's chunk mismatch the reader, which raises the LOGICAL_ERROR exception "Invalid number of
-- columns in chunk pushed to OutputPort".
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER (), 1 AS a, 'z' AS b FROM (SELECT 0, 5 FROM t_window_const);

-- The projection is constant at runtime but is not folded into a literal: identity returns its
-- constant argument unchanged.
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) AS c, s FROM t_window_const);

-- A constant-output projection that references a source column: ignore(s) always returns 0.
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) AS c, s FROM t_window_const);

-- UNION ALL subquery: the branches are read as plain projections while the Union and the window run
-- on the coordinator, so no const-projecting mergeable-state read is involved.
SELECT count(*) OVER () FROM (SELECT 0 FROM t_window_const UNION ALL SELECT 0 FROM t_window_const) ORDER BY 1 LIMIT 1;
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c FROM t_window_const UNION ALL SELECT 1 AS c FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const UNION ALL SELECT 0 AS c, s FROM t_window_const);

-- A bare IN is genuinely non-constant, so it stays a plain column. Planned against a missing
-- cluster, it must reach the cluster lookup and report CLUSTER_DOESNT_EXIST, which shows the header
-- was built rather than failing earlier.
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN (SELECT toString(number) FROM numbers(1)) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN ('1', '2') FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT toUInt8(s IN ('1', '2')) + 0 FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- ignore and indexHint return a constant whatever their argument is, so these stay constant even
-- with an IN inside. Covers an IN tuple, an IN subquery, and nesting under another function.
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN (SELECT toString(number) FROM numbers(3))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT toString(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT indexHint(s IN ('1', '2')) FROM t_window_const);

-- identity passes its argument through, so over a constant argument it is constant too. Covers an
-- IN tuple, an IN subquery, and nested wrappers.
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN (SELECT toString(number) FROM numbers(3)))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN ('1', '2'))) AS c, s FROM t_window_const);

-- __scalarSubqueryResult is the internal pass-through sibling of identity and behaves the same way
-- over a constant argument. Covers an IN tuple, an IN subquery, and either nesting order.
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(ignore(s IN (SELECT toString(number) FROM numbers(3)))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(__scalarSubqueryResult(ignore(s IN ('1', '2')))) FROM t_window_const);

-- __applyFilter with an empty filter id returns a constant whatever its key is, so the key may hold
-- an IN. Covers an IN tuple, an IN subquery, an always-constant key, a nested wrapper, and either
-- nesting order with identity.
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', s IN (SELECT toString(number) FROM numbers(3))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', ignore(s)) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(__applyFilter('', s IN ('1', '2'))) FROM t_window_const);

SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', identity(ignore(s IN (SELECT toString(number) FROM numbers(3))))) FROM t_window_const);

-- A function can be constant from only a subset of constant arguments: if(false, non-const, const)
-- yields the constant else branch, and(.., false) yields false, or(.., true) yields true.
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN ('1', '2')), toUInt8(s IN ('1', '2')), 1) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) AND (s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT (s IN ('1', '2')) OR (NOT ignore(s IN ('1', '2'))) FROM t_window_const);

-- The same shape when the chosen branch is non-constant: it must stay a plain column.
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN ('1', '2')), 7, toUInt8(s IN ('1', '2'))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- A pass-through wrapper over a non-constant argument must NOT be treated as constant.
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(s IN ('1', '2')) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(toUInt8(s IN ('1', '2'))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- The set side of an IN can be a subquery rather than a literal tuple, and such a node has no scalar
-- result type. These non-constant shapes must still reach the cluster lookup: a bare IN subquery, a
-- wrapper over one, and a function whose chosen branch is one.
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN (SELECT toString(number) FROM numbers(2)) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(s IN (SELECT toString(number) FROM numbers(2))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN (SELECT toString(number) FROM numbers(2))), 7, toUInt8(s IN (SELECT toString(number) FROM numbers(2)))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- The constant counterpart of that shape, with an IN subquery as a sibling argument.
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN (SELECT toString(number) FROM numbers(2))) AND (s IN (SELECT toString(number) FROM numbers(2))) FROM t_window_const);

-- A higher-order function over a constant array whose lambda body is constant is itself constant.
-- The lambda argument has a function type that cannot be materialized as a column, so these shapes
-- exercise that path. Covers an IN tuple, an IN subquery, a mixed list, and arrayFilter.
SELECT DISTINCT count(*) OVER () FROM (SELECT arrayMap(x -> ignore(s IN ('1', '2')), [1]) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT arrayMap(x -> ignore(s IN (SELECT toString(number) FROM numbers(3))), [1]) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT arrayMap(x -> ignore(s IN ('1', '2')), [1]) AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT arrayFilter(x -> ignore(s IN ('1', '2')), [1]) FROM t_window_const);

-- A higher-order function whose lambda body is non-constant must stay a plain column.
SELECT DISTINCT count(*) OVER () FROM (SELECT arrayMap(x -> toUInt8(s IN ('1', '2')), [1]) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- Every assertion above is a row count, which a local plan returns just as well, so on their own they
-- stay green even when parallel replicas are declined and no mergeable-state read happens. The first
-- column keeps this from passing vacuously when the filter below matches nothing.
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, countIf(ProfileEvents['ParallelReplicasUsedCount'] = 0) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND initial_query_id = query_id AND has(tables, currentDatabase() || '.t_window_const') SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_window_const;
