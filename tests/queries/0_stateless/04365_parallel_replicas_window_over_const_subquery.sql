-- Tags: replica

DROP TABLE IF EXISTS t_window_const;
CREATE TABLE t_window_const (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_const SELECT toString(number) FROM numbers(2000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

-- A window function that does not reference the constant column projected by the inner
-- subquery used to raise a LOGICAL_ERROR exception "Invalid number of columns in chunk
-- pushed to OutputPort": the coordinator computed an empty mergeable header while a replica
-- streamed the constant column.
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER (), 1 AS a, 'z' AS b FROM (SELECT 0, 5 FROM t_window_const);

-- The projection is constant at runtime but not folded into a ConstantNode: identity is not
-- suitable for constant folding, yet it returns its constant argument unchanged (a ColumnConst).
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) AS c, s FROM t_window_const);

-- A constant-output projection that references a source column: ignore(s) is not suitable
-- for constant folding (stays a FunctionNode) yet always returns a ColumnConst(UInt8).
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) AS c, s FROM t_window_const);

-- UNION ALL subquery: each branch is delegated to the replicas as a plain projection
-- (SELECT 0 FROM t) while the Union/Window run on the coordinator, so the window is not
-- pushed into a const-projecting mergeable-state read and the header cannot diverge here.
SELECT count(*) OVER () FROM (SELECT 0 FROM t_window_const UNION ALL SELECT 0 FROM t_window_const) ORDER BY 1 LIMIT 1;
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c FROM t_window_const UNION ALL SELECT 1 AS c FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const UNION ALL SELECT 0 AS c, s FROM t_window_const);

-- A projection that holds an IN operator or a subquery is not a constant column and cannot be
-- evaluated while building the analyze-only header (the prepared set is not registered there).
-- It used to raise a LOGICAL_ERROR exception "No set is registered for key ...". The query is
-- planned against a missing cluster so it deterministically reports CLUSTER_DOESNT_EXIST after
-- the header is built, proving the analyze header construction no longer aborts.
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN (SELECT toString(number) FROM numbers(1)) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN ('1', '2') FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT toUInt8(s IN ('1', '2')) + 0 FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- An always-constant function (ignore/indexHint) is a ColumnConst on the replicas regardless of
-- its arguments, even when the argument subtree holds an IN operator or subquery that cannot be
-- evaluated while building the analyze-only header. The header must keep the constant, otherwise the
-- unused column is pruned on the coordinator while the replica streams it (the same OutputPort
-- divergence). This holds for an IN tuple, an IN subquery, and an always-constant function nested
-- under another constant-folding function.
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN (SELECT toString(number) FROM numbers(3))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT toString(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT indexHint(s IN ('1', '2')) FROM t_window_const);

-- A transparent wrapper (identity) is not suitable for constant folding, yet it returns its argument
-- column unchanged, so over a constant argument it emits a ColumnConst on the replicas just like the
-- argument does. When that argument holds an IN operator (so it cannot be evaluated while building the
-- analyze-only header), the header must still keep the constant, otherwise the unused column is pruned
-- on the coordinator while the replica streams it (the same OutputPort divergence). This holds for an
-- IN tuple, an IN subquery, and nested wrappers.
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN (SELECT toString(number) FROM numbers(3)))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(ignore(s IN ('1', '2'))) AS c, s FROM t_window_const);

-- The transparent wrapper recognition is class-based (FunctionIdentityBase), not by name, so the
-- sibling internal wrapper __scalarSubqueryResult is covered too: it is not suitable for constant
-- folding, yet it returns its argument column unchanged, so over a constant argument it emits a
-- ColumnConst on the replicas. With an IN-holding always-constant argument, the analyze-only header
-- must keep the constant, otherwise the unused column is pruned on the coordinator while the replica
-- streams it (the same OutputPort divergence). This holds for an IN tuple, an IN subquery, and either
-- nesting order with identity.
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(ignore(s IN ('1', '2'))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(ignore(s IN (SELECT toString(number) FROM numbers(3)))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(__scalarSubqueryResult(ignore(s IN ('1', '2')))) FROM t_window_const);

-- The constant-by-semantics recognition is not limited to a fixed set of function names: any
-- non-foldable function whose result is a ColumnConst regardless of its arguments is kept constant by
-- executing it against placeholder argument columns (the same way real header evaluation does). The
-- internal direct-callable __applyFilter('', key) returns a ColumnConst(UInt8) for an empty filter id
-- regardless of its key argument, so over an IN-holding key (which cannot be evaluated while building
-- the analyze-only header) the header must still keep the constant, otherwise the unused column is
-- pruned on the coordinator while the replica streams it (the same OutputPort divergence). This holds
-- for an IN tuple, an IN subquery, an always-constant key, a nested transparent wrapper, and either
-- nesting order with identity.
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', s IN (SELECT toString(number) FROM numbers(3))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', ignore(s)) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', identity(ignore(s IN ('1', '2')))) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(__applyFilter('', s IN ('1', '2'))) FROM t_window_const);

-- __applyFilter with an empty id is constant regardless of its key, so a key that itself holds an IN
-- subquery still keeps the constant header.
SELECT DISTINCT count(*) OVER () FROM (SELECT __applyFilter('', identity(ignore(s IN (SELECT toString(number) FROM numbers(3))))) FROM t_window_const);

-- A partial-constant function produces a ColumnConst from only a subset of constant arguments:
-- if(const false, non-const, const) yields the const else branch, and(.., const false) yields false,
-- or(.., const true) yields true. The real projection plan derives this via
-- getConstantResultForNonConstArguments, so the analyze-only header must keep it constant even though
-- a sibling argument holds an unevaluable IN.
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN ('1', '2')), toUInt8(s IN ('1', '2')), 1) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN ('1', '2')) AND (s IN ('1', '2')) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT (s IN ('1', '2')) OR (NOT ignore(s IN ('1', '2'))) FROM t_window_const);

-- A partial-constant function that does not resolve to a constant (the chosen branch is non-constant)
-- stays a plain column: if(const false, .., non-const else) is non-constant, so it takes the
-- plain-column path and reports CLUSTER_DOESNT_EXIST against a missing cluster after the header is built.
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN ('1', '2')), 7, toUInt8(s IN ('1', '2'))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- identity over a non-constant IN argument stays a plain column (not falsely kept constant): it goes
-- the same plain-column header path as a bare IN, so it cannot be evaluated here and deterministically
-- reports CLUSTER_DOESNT_EXIST against a missing cluster after the header is built.
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(s IN ('1', '2')) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
-- A transparent wrapper over a genuinely non-constant argument also stays a plain column: its argument
-- does not fold to a constant, so the wrapper is not falsely kept constant.
SELECT DISTINCT count(*) OVER () FROM (SELECT __scalarSubqueryResult(toUInt8(s IN ('1', '2'))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- A non-constant projection whose IN operator takes a SUBQUERY (not a literal tuple) on its set side must
-- stay on the plain-column header path. The set side is a subquery node with no scalar result type here,
-- so folding it must not ask that node for its result type. These shapes used to raise a getResultType
-- UNSUPPORTED_METHOD exception instead of reaching the missing-cluster check; they must report
-- CLUSTER_DOESNT_EXIST after the header is built. This covers a bare IN subquery, a transparent wrapper
-- over an IN subquery, and a partial-constant function whose chosen branch is a non-constant IN subquery.
SELECT DISTINCT count(*) OVER () FROM (SELECT s IN (SELECT toString(number) FROM numbers(2)) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(s IN (SELECT toString(number) FROM numbers(2))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }
SELECT DISTINCT count(*) OVER () FROM (SELECT if(ignore(s IN (SELECT toString(number) FROM numbers(2))), 7, toUInt8(s IN (SELECT toString(number) FROM numbers(2)))) FROM t_window_const) SETTINGS cluster_for_parallel_replicas = 'not_exists'; -- { serverError CLUSTER_DOESNT_EXIST }

-- The constant counterpart of the same shapes: when an always-constant function (ignore) or a
-- partial-constant function resolves to a ColumnConst despite a SUBQUERY IN sibling argument, the
-- analyze-only header keeps the constant just like the real replica projection, so the query runs.
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s IN (SELECT toString(number) FROM numbers(2))) AND (s IN (SELECT toString(number) FROM numbers(2))) FROM t_window_const);

DROP TABLE t_window_const;
