-- a filter fused into LEFT ARRAY JOIN gives the same rows as the filter above it, padded defaults included

-- the planner does not fuse the filter for serialized plans
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_fuse_left;
CREATE TABLE t_fuse_left (id UInt64, arr Array(UInt64), s Array(String), n Array(Nullable(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse_left SELECT number, if(number % 3 = 0, [], range(number % 5)), if(number % 4 = 0, [], arrayMap(x -> toString(x), range(number % 3))), if(number % 2 = 0, [], [NULL, 'a']) FROM numbers(1000);

SELECT count() FROM (EXPLAIN actions = 1 SELECT id FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 0) WHERE explain ILIKE '%Element filter column%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS x FROM t_fuse_left WHERE x = 0) WHERE explain ILIKE '%Element filter column%';

SELECT 'padded default matches';
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 0;
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 0 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count(), sum(cityHash64(id, y)) FROM t_fuse_left LEFT ARRAY JOIN s AS y WHERE y = '';
SELECT count(), sum(cityHash64(id, y)) FROM t_fuse_left LEFT ARRAY JOIN s AS y WHERE y = '' SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count(), sum(cityHash64(id)) FROM t_fuse_left LEFT ARRAY JOIN n AS z WHERE z IS NULL;
SELECT count(), sum(cityHash64(id)) FROM t_fuse_left LEFT ARRAY JOIN n AS z WHERE z IS NULL SETTINGS query_plan_fuse_filter_into_array_join = 0;

SELECT 'padded default does not match';
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 3;
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 3 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = id % 5;
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = id % 5 SETTINGS query_plan_fuse_filter_into_array_join = 0;

SELECT 'two arrays';
SELECT count(), sum(cityHash64(id, x, w)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x, arrayMap(v -> v * 2, arr) AS w WHERE w = 2 * x AND x != 1;
SELECT count(), sum(cityHash64(id, x, w)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x, arrayMap(v -> v * 2, arr) AS w WHERE w = 2 * x AND x != 1 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x, s AS y WHERE x = 0 OR y = '' SETTINGS enable_unaligned_array_join = 1;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x, s AS y WHERE x = 0 OR y = '' SETTINGS enable_unaligned_array_join = 1, query_plan_fuse_filter_into_array_join = 0;

SELECT 'function form';
SELECT count(), sum(cityHash64(id, x)) FROM (SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS x FROM t_fuse_left) WHERE x = 0;
SELECT count(), sum(cityHash64(id, x)) FROM (SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS x FROM t_fuse_left) WHERE x = 0 SETTINGS query_plan_fuse_filter_into_array_join = 0;

SELECT 'order by limit';
SELECT groupArray(id) FROM (SELECT id FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 3 ORDER BY id LIMIT 5);
SELECT groupArray(id) FROM (SELECT id FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 3 ORDER BY id LIMIT 5) SETTINGS query_plan_fuse_filter_into_array_join = 0;

SELECT 'small blocks';
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 0 OR x = 3 SETTINGS max_block_size = 1;
SELECT count(), sum(cityHash64(id, x)) FROM t_fuse_left LEFT ARRAY JOIN arr AS x WHERE x = 0 OR x = 3 SETTINGS max_block_size = 1, query_plan_fuse_filter_into_array_join = 0;

DROP TABLE t_fuse_left;
