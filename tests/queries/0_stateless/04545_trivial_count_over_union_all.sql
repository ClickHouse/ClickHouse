-- Tags: no-parallel-replicas
-- ^ trivial count over UNION ALL disables parallel replicas locally; pin it out for a stable plan.

DROP TABLE IF EXISTS t_count_union_a;
DROP TABLE IF EXISTS t_count_union_b;

CREATE TABLE t_count_union_a (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_count_union_b (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;

INSERT INTO t_count_union_a SELECT number FROM numbers(1000);
INSERT INTO t_count_union_b SELECT number FROM numbers(500);

SET optimize_trivial_count_query = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projections = 0;

-- Optimization fires: metadata-only count, no ReadFromMergeTree in the plan.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%Optimized trivial count over UNION ALL%';
SELECT count()
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';

-- Correctness.
SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b);

-- The extra work is real: with the optimization the max_rows_to_read limit is not hit.
SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b) SETTINGS max_rows_to_read = 1;

-- Three branches and nested UNION ALL are also handled.
SELECT count()
FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b);
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b)))
WHERE explain ILIKE '%Optimized trivial count over UNION ALL%';

-- Negative cases: the optimization must NOT fire (values, not just counts, matter), so the
-- branches are read. Assert a ReadFromMergeTree is present and the result stays correct.
-- UNION DISTINCT.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a UNION DISTINCT SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM (SELECT id FROM t_count_union_a UNION DISTINCT SELECT id FROM t_count_union_b);
-- INTERSECT.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a INTERSECT SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM (SELECT id FROM t_count_union_a INTERSECT SELECT id FROM t_count_union_b);
-- EXCEPT.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a EXCEPT SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM (SELECT id FROM t_count_union_a EXCEPT SELECT id FROM t_count_union_b);
-- A branch with WHERE reshapes its count, so the optimization is not applied but the result is correct.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a WHERE id >= 10 UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM (SELECT id FROM t_count_union_a WHERE id >= 10 UNION ALL SELECT id FROM t_count_union_b);
-- A branch with LIMIT.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a LIMIT 5 UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
-- count(DISTINCT) over UNION ALL is not a trivial count.
SELECT count() > 0
FROM (EXPLAIN SELECT count(DISTINCT id) FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%';
-- Kill switch: with optimize_trivial_count_query = 0 the branches are read.
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM (SELECT id FROM t_count_union_a UNION ALL SELECT id FROM t_count_union_b))
WHERE explain ILIKE '%ReadFromMergeTree%'
SETTINGS optimize_trivial_count_query = 0;

DROP TABLE t_count_union_a;
DROP TABLE t_count_union_b;
