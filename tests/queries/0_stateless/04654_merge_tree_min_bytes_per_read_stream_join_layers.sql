-- Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

DROP TABLE IF EXISTS layer_left;
DROP TABLE IF EXISTS layer_right;

CREATE TABLE layer_left (k UInt64, w UInt8) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
CREATE TABLE layer_right (k UInt64, w UInt8, payload String) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO layer_left SELECT number, number % 251 FROM numbers(600000);
INSERT INTO layer_right SELECT number, number % 251, repeat('x', 512) FROM numbers(600000);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 1;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET use_statistics = 0;
SET max_threads = 64;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_read_task_size = 1;

-- Verify that this test actually reaches `optimizeJoinByShards` and layered reading.
SELECT count() > 0
FROM (EXPLAIN PLAN
    SELECT sum(l.w + r.w)
    FROM layer_left AS l INNER JOIN layer_right AS r USING k)
WHERE explain LIKE '%Sharding:%';

-- Both inputs are narrow, so their combined volume should reduce the common layer count.
WITH
    (SELECT sum(if(match(explain, 'MergeTreeSelect.*× (\\d+)'), toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')), 1))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w)
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 65536)
     WHERE explain LIKE '%MergeTreeSelect%') AS capped,
    (SELECT sum(if(match(explain, 'MergeTreeSelect.*× (\\d+)'), toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')), 1))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w)
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 0)
     WHERE explain LIKE '%MergeTreeSelect%') AS uncapped
SELECT capped < uncapped;

-- A small input must not throttle a large input. The combined payload is large enough to retain all layers.
WITH
    (SELECT sum(if(match(explain, 'MergeTreeSelect.*× (\\d+)'), toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')), 1))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w + cityHash64(r.payload))
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 65536)
     WHERE explain LIKE '%MergeTreeSelect%') AS capped,
    (SELECT sum(if(match(explain, 'MergeTreeSelect.*× (\\d+)'), toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')), 1))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w + cityHash64(r.payload))
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 0)
     WHERE explain LIKE '%MergeTreeSelect%') AS uncapped
SELECT capped = uncapped;

SELECT
    (SELECT sum(l.w + r.w) FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 65536)
    =
    (SELECT sum(l.w + r.w) FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS merge_tree_min_bytes_per_read_stream = 0);

DROP TABLE layer_left;
DROP TABLE layer_right;
