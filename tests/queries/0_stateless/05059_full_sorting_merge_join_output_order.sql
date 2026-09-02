-- A full sorting merge join emits its result in join-key order, so a sort by that key above it (the pre-sort
-- of the next merge join in a chain, or an `ORDER BY`) must degrade to a merge of the already sorted streams
-- instead of sorting from scratch. https://github.com/ClickHouse/ClickHouse/issues/117679
--
-- Randomized in CI and pinned here so the plan shape is the one described: `join_algorithm`, `max_threads`,
-- `optimize_read_in_order`, `query_plan_join_shard_by_pk_ranges` (both values are exercised explicitly) and
-- the join-order settings.

DROP TABLE IF EXISTS fsmj_order_a;
DROP TABLE IF EXISTS fsmj_order_b;
DROP TABLE IF EXISTS fsmj_order_c;

CREATE TABLE fsmj_order_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE fsmj_order_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE fsmj_order_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

-- Duplicate keys, gaps on every side, and several parts on the left table.
INSERT INTO fsmj_order_a SELECT number % 700, number FROM numbers(1000);
INSERT INTO fsmj_order_a SELECT number % 700, number FROM numbers(1000, 500);
INSERT INTO fsmj_order_b SELECT number % 500 * 2, number FROM numbers(800);
INSERT INTO fsmj_order_c SELECT number % 300 * 3, number FROM numbers(600);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 0;

-- Plan level: `Sort description:` is printed by a full sort, `Prefix sort description:` by a sort that
-- merges already ordered input. A three-table chain has four merge-join sorts, none of them full.
SELECT 'inner chain plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id);

-- Pipeline level: no `MergeSortingTransform` for two `MergeJoinTransform`s.
SELECT 'inner chain pipeline', countIf(explain LIKE '%MergeSortingTransform%'), countIf(explain LIKE '%MergeJoinTransform%')
FROM (EXPLAIN PIPELINE
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id);

-- A LEFT join keeps the left key order, a RIGHT join keeps the right key order.
SELECT 'left chain plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id);

SELECT 'right chain plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a RIGHT JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON b.id = c.id);

-- The key of the other side is not ordered after an outer join (non-matched rows carry defaults), and a
-- FULL join orders neither side, so these keep one full sort.
SELECT 'left chain other side plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON b.id = c.id);

SELECT 'full chain plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.v) + sum(b.v) + sum(c.v)
    FROM fsmj_order_a AS a FULL JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id);

-- An `ORDER BY` the join key above a merge join is a merge as well, and it stays correct when the order
-- has to be finished by a suffix column.
SELECT 'order by plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT a.id, b.v FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id ORDER BY a.id, b.v);

WITH (SELECT groupArray((id, v)) FROM (SELECT a.id AS id, b.v AS v FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id ORDER BY a.id, b.v)) AS rows
SELECT 'order by result', length(rows), rows = arraySort(rows);

-- When the join keys are not the sorting key of the tables, the table-side sorts stay full sorts, but the
-- sort above the first join is still a merge.
SELECT 'unsorted keys chain plan', countIf(explain LIKE '%Sort description:%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(a.id) FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.v = b.v INNER JOIN fsmj_order_c AS c ON a.v = c.v);

-- `parallel_full_sorting_merge` is sharded by the hash of the keys instead, on every level of the chain:
-- every merge-join sort is scattered and sorted from scratch.
SELECT 'parallel unsorted keys chain pipeline', countIf(explain LIKE '%ScatterByPartitionTransform%'), countIf(explain LIKE '%MergeSortingTransform%'), countIf(explain LIKE '%MergingSortedTransform%')
FROM (EXPLAIN PIPELINE
    SELECT sum(a.id) FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.v = b.v INNER JOIN fsmj_order_c AS c ON a.v = c.v
    SETTINGS join_algorithm = 'parallel_full_sorting_merge');

-- Results match the hash join, with and without sharding the joins by primary-key ranges.
SELECT 'inner', count(), sum(a.v), sum(b.v), sum(c.v)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS join_algorithm = 'hash';
SELECT 'inner', count(), sum(a.v), sum(b.v), sum(c.v)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id;
SELECT 'inner', count(), sum(a.v), sum(b.v), sum(c.v)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;

SELECT 'left', count(), sum(a.v), sum(b.v), sum(c.v), countIf(b.id = 0), countIf(c.id = 0)
FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS join_algorithm = 'hash';
SELECT 'left', count(), sum(a.v), sum(b.v), sum(c.v), countIf(b.id = 0), countIf(c.id = 0)
FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id;
SELECT 'left', count(), sum(a.v), sum(b.v), sum(c.v), countIf(b.id = 0), countIf(c.id = 0)
FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;

SELECT 'right', count(), sum(a.v), sum(b.v), sum(c.v), countIf(a.id = 0)
FROM fsmj_order_a AS a RIGHT JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON b.id = c.id
SETTINGS join_algorithm = 'hash';
SELECT 'right', count(), sum(a.v), sum(b.v), sum(c.v), countIf(a.id = 0)
FROM fsmj_order_a AS a RIGHT JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON b.id = c.id;
SELECT 'right', count(), sum(a.v), sum(b.v), sum(c.v), countIf(a.id = 0)
FROM fsmj_order_a AS a RIGHT JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON b.id = c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;

SELECT 'left use nulls', count(), sum(a.v), sum(b.v), sum(c.v), countIf(b.id IS NULL), countIf(c.id IS NULL)
FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT 'left use nulls', count(), sum(a.v), sum(b.v), sum(c.v), countIf(b.id IS NULL), countIf(c.id IS NULL)
FROM fsmj_order_a AS a LEFT JOIN fsmj_order_b AS b ON a.id = b.id LEFT JOIN fsmj_order_c AS c ON a.id = c.id
SETTINGS join_use_nulls = 1;

SELECT 'unsorted keys', count(), sum(a.id), sum(b.id), sum(c.id)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.v = b.v INNER JOIN fsmj_order_c AS c ON a.v = c.v
SETTINGS join_algorithm = 'hash';
SELECT 'unsorted keys', count(), sum(a.id), sum(b.id), sum(c.id)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.v = b.v INNER JOIN fsmj_order_c AS c ON a.v = c.v;
SELECT 'unsorted keys', count(), sum(a.id), sum(b.id), sum(c.id)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.v = b.v INNER JOIN fsmj_order_c AS c ON a.v = c.v
SETTINGS join_algorithm = 'parallel_full_sorting_merge';

-- With `query_plan_join_shard_by_pk_ranges` the lower join is sharded by primary-key ranges, while the upper
-- join, whose right key is not the primary key of its table, stays a single merge join: the merge-join sort
-- between them has to merge the shards back into one stream instead of keeping one stream per shard.
SELECT 'pk range sharding mixed plan', countIf(explain LIKE '%Sharding:%')
FROM (EXPLAIN actions = 1
    SELECT count() FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.v
    SETTINGS query_plan_join_shard_by_pk_ranges = 1);

SELECT 'pk range sharding mixed', count(), sum(a.v), sum(b.v), sum(c.id)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.v
SETTINGS join_algorithm = 'hash';
SELECT 'pk range sharding mixed', count(), sum(a.v), sum(b.v), sum(c.id)
FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id INNER JOIN fsmj_order_c AS c ON a.id = c.v
SETTINGS query_plan_join_shard_by_pk_ranges = 1;

-- The same with the lower join in a sorted subquery.
SELECT 'pk range sharding sorted subquery', count(), sum(s.k), sum(c.id)
FROM (SELECT a.id AS k FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id ORDER BY a.id) AS s
INNER JOIN fsmj_order_c AS c ON s.k = c.v
SETTINGS join_algorithm = 'hash';
SELECT 'pk range sharding sorted subquery', count(), sum(s.k), sum(c.id)
FROM (SELECT a.id AS k FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id ORDER BY a.id) AS s
INNER JOIN fsmj_order_c AS c ON s.k = c.v
SETTINGS query_plan_join_shard_by_pk_ranges = 1;
SELECT 'pk range sharding sorted subquery', count(), sum(s.k), sum(c.id)
FROM (SELECT a.id AS k FROM fsmj_order_a AS a INNER JOIN fsmj_order_b AS b ON a.id = b.id ORDER BY a.id) AS s
INNER JOIN fsmj_order_c AS c ON s.k = c.v
SETTINGS query_plan_join_shard_by_pk_ranges = 1, join_algorithm = 'parallel_full_sorting_merge';

DROP TABLE fsmj_order_a;
DROP TABLE fsmj_order_b;
DROP TABLE fsmj_order_c;
