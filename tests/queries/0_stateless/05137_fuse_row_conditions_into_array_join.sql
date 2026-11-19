-- A filter comparing an ARRAY JOIN element with a column of the row is fused into the step.

-- fusion is skipped for serialized plans, pin it so the plan-shape checks hold in the distributed-plan suite
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_aj_row;
CREATE TABLE t_aj_row (key String, n UInt8, nk Nullable(String), lc LowCardinality(String), arr Array(String), payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_aj_row SELECT toString(number % 5), number % 3, if(number % 4 = 0, NULL, toString(number % 5)), toString(number % 2), arrayMap(x -> toString(x), range(number % 6)), repeat('p', 10) FROM numbers(100);

-- element vs row column: fused, same result as unfused
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key) WHERE explain ILIKE '%Element filter column%';
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key AND payload != '';
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key AND payload != '' SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE toUInt8(x) + n > 4;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE toUInt8(x) + n > 4 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key OR n = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key OR n = 0 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = nk;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = nk SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = lc;
SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = lc SETTINGS query_plan_fuse_filter_into_array_join = 0;
-- filter column kept for the projection
SELECT sum(x = key), count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key OR n = 0;
SELECT sum(x = key), count() FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key OR n = 0 SETTINGS query_plan_fuse_filter_into_array_join = 0;
-- arrayFilter oracle
SELECT sum(length(arrayFilter(x -> x = key, arr))) FROM t_aj_row;

-- the lift must not prune columns the fused filter reads
SELECT count(), sum(cityHash64(key)) FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key;
SELECT count(), sum(cityHash64(key)) FROM t_aj_row ARRAY JOIN arr AS x WHERE x = key SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT sum(cityHash64(payload)) FROM t_aj_row ARRAY JOIN arr AS x WHERE x = toString(n);
SELECT sum(cityHash64(payload)) FROM t_aj_row ARRAY JOIN arr AS x WHERE x = toString(n) SETTINGS query_plan_fuse_filter_into_array_join = 0;

-- lowered chain: the outer element is a row column for the inner step
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT arrayJoin(arr) AS a, arrayJoin(arrayReverse(arr)) AS b FROM t_aj_row WHERE a < b) SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Element filter column%';
SELECT count() FROM (SELECT arrayJoin(arr) AS a, arrayJoin(arrayReverse(arr)) AS b FROM t_aj_row WHERE a < b) SETTINGS query_plan_lower_array_join_function = 1;
SELECT count() FROM (SELECT arrayJoin(arr) AS a, arrayJoin(arrayReverse(arr)) AS b FROM t_aj_row WHERE a < b) SETTINGS query_plan_lower_array_join_function = 1, query_plan_fuse_filter_into_array_join = 0;

-- no element read: not fused
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_aj_row ARRAY JOIN arr AS x WHERE n = 1 SETTINGS query_plan_filter_push_down = 0) WHERE explain ILIKE '%Element filter column%';
-- LEFT: not fused
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_aj_row LEFT ARRAY JOIN arr AS x WHERE x = key) WHERE explain ILIKE '%Element filter column%';
SELECT count() FROM t_aj_row LEFT ARRAY JOIN arr AS x WHERE x = key;

DROP TABLE t_aj_row;
