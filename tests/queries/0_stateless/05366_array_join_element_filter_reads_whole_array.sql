-- a fused element filter that reads the whole array must not copy the array once per element

SET enable_lazy_columns_replication = 1;
-- the planner does not fuse the filter for serialized plans
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_aj_whole;
CREATE TABLE t_aj_whole (id String, items Array(String), tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aj_whole SELECT '1', arrayMap(i -> concat('item-', toString(i)), range(8000)), ['tag'];

SELECT count() FROM (EXPLAIN actions = 1 SELECT id, item, tag FROM t_aj_whole ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1')) WHERE explain ILIKE '%Element filter column%';

-- a copy per element needs about 1 GiB
SET max_memory_usage = '100Mi';
SELECT count(), sum(item = 'item-1') FROM t_aj_whole ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1');
SELECT count(), sum(item = 'item-1') FROM t_aj_whole ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE length(items) > 100 OR item = 'item-1';
SELECT count(), sum(item = 'item-1') FROM t_aj_whole ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (arrayExists(x -> x = id, items) AND item = 'item-1');
-- the arrayJoin function, lowered to the same steps
SELECT count(), sum(item = 'item-1') FROM (SELECT id, items, arrayJoin(items) AS item, arrayJoin(tags) AS tag FROM t_aj_whole) WHERE NOT (has(items, 'item-1') AND item = 'item-1');
SELECT count(), sum(item = 'item-1') FROM t_aj_whole ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1') SETTINGS query_plan_fuse_filter_into_array_join = 0;

DROP TABLE t_aj_whole;
