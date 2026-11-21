-- A row-only part of a filter fused into ARRAY JOIN must be computed before the ARRAY JOIN, not broadcast to every element.

-- fusion is skipped for serialized plans, pin it so the plan-shape checks hold in the distributed-plan suite
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_aj_lift;
CREATE TABLE t_aj_lift (id String, items Array(String), tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aj_lift SELECT '1', arrayMap(i -> concat('item-', toString(i)), range(8000)), ['tag'];

-- the filter is fused, and `has` is computed once, below the ARRAY JOIN, instead of inside the element filter
SELECT count() FROM (EXPLAIN compact = 0, actions = 1 SELECT id, item, tag FROM t_aj_lift ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1')) WHERE explain ILIKE '%Element filter column%';
SELECT count() FROM (EXPLAIN compact = 0, actions = 1 SELECT id, item, tag FROM t_aj_lift ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1')) WHERE explain ILIKE '%FUNCTION has(%';

SELECT count() FROM t_aj_lift ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1') SETTINGS max_memory_usage = '100Mi';
SELECT count() FROM t_aj_lift ARRAY JOIN items AS item ARRAY JOIN tags AS tag WHERE NOT (has(items, 'item-1') AND item = 'item-1') SETTINGS query_plan_fuse_filter_into_array_join = 0;

DROP TABLE t_aj_lift;
