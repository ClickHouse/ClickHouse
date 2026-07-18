-- Tags: no-parallel-replicas
-- The EXPLAIN PLAN assertions below depend on the count()-over-ARRAY-JOIN rewrite, which is a
-- new-analyzer pass; the plan shape differs under parallel replicas.

-- Tests that count() over an ARRAY JOIN, whose element values are never referenced, is rewritten to
-- sum() over the array lengths so only the lightweight arr.size0 subcolumn is read instead of the
-- whole array. See issue #110812.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_parallel_replicas = 0;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projections = 0;
-- The rewrite declines under unaligned array join (row count is the max length, not one array's
-- length), so pin it off to keep the EXPLAIN assertions independent of randomized settings.
SET enable_unaligned_array_join = 0;

DROP TABLE IF EXISTS t_count_aj;
CREATE TABLE t_count_aj (id UInt64, arr Array(UInt64), narr Array(Nullable(String)), lcarr Array(LowCardinality(String)), m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;

-- Include empty arrays to exercise LEFT ARRAY JOIN semantics.
INSERT INTO t_count_aj
SELECT number, if(number % 7 = 0, [], range(number % 10)),
       arrayMap(x -> if(x % 2 = 0, NULL, toString(x)), range(number % 5)),
       arrayMap(x -> toString(x % 3), range(number % 4)),
       (SELECT map('k1', number, 'k2', number + 1))
FROM numbers(1000);

-- Correctness: count() must be unchanged and equal to sum(length(arr)) / sum(greatest(length(arr), 1)) for LEFT.
SELECT count() FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(if(empty(arr), 1, length(arr))) FROM t_count_aj) FROM t_count_aj LEFT ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(length(narr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN narr;
SELECT count() = (SELECT sum(length(lcarr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN lcarr;
SELECT count() = (SELECT sum(length(m)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN m;
-- count(*) and count(1) are also plain row counts.
SELECT count(*) = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count(1) = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
-- count(NULL) always returns 0 (a NULL argument is never counted); the rewrite must NOT fire for it.
SELECT count(NULL) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count(NULL::Nullable(UInt64)) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count(NULL) FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';

-- The output column name must remain count() (the rewrite must not rename the projection).
DESCRIBE (SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) FORMAT TSVRaw;

-- Optimization: the plan must read arr.size0 and no longer contain an ARRAY JOIN step.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj LEFT ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN narr) WHERE explain ILIKE '%narr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN lcarr) WHERE explain ILIKE '%lcarr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN m) WHERE explain ILIKE '%m.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%ARRAY JOIN%';
-- No placeholder array is materialized: the plan must aggregate with sum() and must not contain
-- arrayWithConstant. This is what makes the rewrite immune to the array-size cap that a materialized
-- placeholder array would hit for a large count() (issue #110812).
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%sum(%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arrayWithConstant%';

-- Finding 2 (no regression): when the array is read elsewhere, the rewrite must NOT fire, so the plan
-- keeps the ARRAY JOIN and never adds a synthetic array. Results must be unchanged.
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(value) FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr) WHERE explain ILIKE '%arrayWithConstant%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%ARRAY JOIN%';
-- GROUP BY over the exploded array (INNER) drops empty-array groups; the count must be unchanged.
SELECT count() FROM (SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr);

-- With the setting disabled, the optimization must not fire (backward compatible).
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%arr.size0%';

DROP TABLE t_count_aj;
