-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag no-parallel-replicas: the query condition cache is populated per replica

-- A lambda that captures nothing is constant-folded into a `COLUMN` node holding a `ColumnFunction`
-- carrier, so a non-deterministic call in its body (`randConstant`) is invisible to a walk over the
-- outer `ActionsDAG` alone. Such a filter must neither write nor reuse query condition cache entries,
-- on a plain read and on an `ORDER BY ... LIMIT` (TopK) read alike, otherwise a verdict that holds for
-- one value of `randConstant` is reused by a later query that draws another value. Today the folded
-- node is already marked as a non-deterministic constant; `isDeterministicAllowingTopKFilter` also
-- looks into the carrier (like `isDeterministic`), and this test pins the combined behavior.

DROP TABLE IF EXISTS t_qcc_folded_lambda;

CREATE TABLE t_qcc_folded_lambda (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192;

-- the query condition cache stores nothing for small tables
INSERT INTO t_qcc_folded_lambda SELECT number, number FROM numbers(1000000);

SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
-- keep the predicate a residual `WHERE`, i.e. on the writer gated by `isDeterministicAllowingTopKFilter`
SET optimize_move_to_prewhere = 0;
SET optimize_use_implicit_projections = 0;

SELECT '= plain read, non-deterministic folded lambda body: not cached =';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_folded_lambda WHERE arrayExists(z -> randConstant(z) % 2 = 0, [b]) FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '= TopK read, non-deterministic folded lambda body: not cached =';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT a FROM t_qcc_folded_lambda WHERE arrayExists(z -> randConstant(z) % 2 = 0, [b]) ORDER BY b LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '= plain read, deterministic folded lambda body: cached =';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_folded_lambda WHERE arrayExists(z -> z = 4242, [b]) FORMAT Null;
SELECT count() > 0 FROM system.query_condition_cache;

DROP TABLE t_qcc_folded_lambda;
