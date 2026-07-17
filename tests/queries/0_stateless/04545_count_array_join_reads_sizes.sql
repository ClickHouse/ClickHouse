-- Tests that count() over ARRAY JOIN, whose element values are never referenced, reads only the
-- lightweight arr.size0 subcolumn instead of materializing the whole array. See issue #110812.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_parallel_replicas = 0;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projections = 0;

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

-- Optimization: the plan must read arr.size0, not the full arr column.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj LEFT ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN narr) WHERE explain ILIKE '%narr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN lcarr) WHERE explain ILIKE '%lcarr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN m) WHERE explain ILIKE '%m.size0%';

-- When the element value IS used, the full array must still be read (optimization must NOT fire).
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(value) FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';

-- With the setting disabled, the optimization must not fire (backward compatible).
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%arr.size0%';

DROP TABLE t_count_aj;
