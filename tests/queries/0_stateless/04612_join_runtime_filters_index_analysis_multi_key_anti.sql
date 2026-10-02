-- Tests for enable_join_runtime_filters_index_analysis on multi-key joins and LEFT ANTI joins.
--
-- Multi-key equi-joins in the `ON a = c AND b = d` form build one per-column runtime filter
-- per key, so left-side index analysis engages on each key. The `ON (a, b) = (c, d)` tuple
-- form instead builds a single filter over the computed `tuple(a, b)` expression, which is not
-- a storage column: index analysis intentionally does not engage there (asserted below as 0/0),
-- while row-level runtime filtering and query results stay correct.
--
-- LEFT ANTI joins build negating (NOT IN) runtime filters: a positive IN-set or range predicate
-- on the left side would be unsound there, so index analysis must stay fail-open and never
-- change results (the negating filter exposes neither recorded key values nor a key range).

DROP TABLE IF EXISTS mk_fact;
DROP TABLE IF EXISTS mk_dim;
DROP TABLE IF EXISTS anti_dim;

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is intentionally disabled under parallel replicas, so pin PR off to
-- exercise the feature (the ParallelReplicas CI job otherwise forces it on).
SET enable_parallel_replicas = 0;

CREATE TABLE mk_fact (a UInt64, b UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 16;
CREATE TABLE mk_dim (a UInt64, b UInt64, tag String)
ENGINE = MergeTree ORDER BY (a, b);
-- The dim rows tagged 'hot' cover only a = 0..63, so the runtime filter on the fact's
-- primary-key prefix `a` can drop the granules with a >= 64.
INSERT INTO mk_fact SELECT number % 100, intDiv(number, 100), number FROM numbers(2000);
INSERT INTO mk_dim SELECT number % 100, intDiv(number, 100), if(number % 100 < 64, 'hot', 'cold') FROM numbers(2000);

-- anti_dim covers only a = 0..49, so `LEFT ANTI JOIN ... ON f.a = d.a` must keep exactly
-- the fact rows with a >= 50. If the negating filter were misused as a positive IN/range
-- predicate for pruning, those granules would be dropped and the count would collapse to 0.
CREATE TABLE anti_dim (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO anti_dim SELECT number % 50, intDiv(number, 50) FROM numbers(1000);

-- Multi-key join, `ON ... AND ...` form
SELECT sum(f.v)
FROM mk_fact AS f
INNER JOIN mk_dim AS d ON f.a = d.a AND f.b = d.b
WHERE d.tag = 'hot'
FORMAT Null
SETTINGS log_comment = '04612_mk_and';

-- Multi-key join, tuple equality form
SELECT sum(f.v)
FROM mk_fact AS f
INNER JOIN mk_dim AS d ON (f.a, f.b) = (d.a, d.b)
WHERE d.tag = 'hot'
FORMAT Null
SETTINGS log_comment = '04612_mk_tuple';

SYSTEM FLUSH LOGS query_log;

-- The `AND` form must consider granules and drop some (1/1); the tuple form must not engage
-- index analysis at all (0/0) because its filter is over the computed tuple expression.
SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04612_mk_and', '04612_mk_tuple')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

-- Correctness: pruning must never change multi-key join results.
SELECT
    (SELECT sum(f.v) FROM mk_fact AS f INNER JOIN mk_dim AS d ON f.a = d.a AND f.b = d.b WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM mk_fact AS f INNER JOIN mk_dim AS d ON f.a = d.a AND f.b = d.b WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);
SELECT
    (SELECT sum(f.v) FROM mk_fact AS f INNER JOIN mk_dim AS d ON (f.a, f.b) = (d.a, d.b) WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM mk_fact AS f INNER JOIN mk_dim AS d ON (f.a, f.b) = (d.a, d.b) WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);

-- LEFT ANTI, single key: the negating filter must not prune the non-matching granules.
-- Exactly the 1000 fact rows with a >= 50 survive the anti join.
SELECT count()
FROM mk_fact AS f
LEFT ANTI JOIN anti_dim AS d ON f.a = d.a;

-- LEFT ANTI, multiple keys: uses a single tuple-based NOT IN runtime filter.
SELECT count()
FROM mk_fact AS f
LEFT ANTI JOIN anti_dim AS d ON f.a = d.a AND f.b = d.b;

-- Correctness: feature off == on for both ANTI shapes.
SELECT
    (SELECT count() FROM mk_fact AS f LEFT ANTI JOIN anti_dim AS d ON f.a = d.a SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mk_fact AS f LEFT ANTI JOIN anti_dim AS d ON f.a = d.a SETTINGS enable_join_runtime_filters_index_analysis = 1);
SELECT
    (SELECT count() FROM mk_fact AS f LEFT ANTI JOIN anti_dim AS d ON f.a = d.a AND f.b = d.b SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mk_fact AS f LEFT ANTI JOIN anti_dim AS d ON f.a = d.a AND f.b = d.b SETTINGS enable_join_runtime_filters_index_analysis = 1);

DROP TABLE mk_fact;
DROP TABLE mk_dim;
DROP TABLE anti_dim;
