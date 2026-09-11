-- A `MATERIALIZED` column with a non-deterministic default keeps its stored value across a
-- `TTL ... GROUP BY ... SET` that rewrites a column its default reads (the whole-stream repair
-- cannot recompute only the rewritten rows). The dependency walk must therefore stop at such a
-- column: neither the sort-key repair nor the unsorted `GROUP BY` aggregation is needed for a value
-- the merge writes unchanged. The deterministic arms are the controls -- there the column IS
-- recomputed and both repairs do run.

DROP TABLE IF EXISTS ttl_nondet_sort_key;
DROP TABLE IF EXISTS ttl_det_sort_key;
DROP TABLE IF EXISTS ttl_nondet_group_by_key;
DROP TABLE IF EXISTS ttl_det_group_by_key;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps the background merge selector off these parts, so
-- the `OPTIMIZE FINAL` below is the only merge and cannot lose the parts to one already running.

-- The sort-key repair: `ORDER BY (g, m)` while `SET x = max(x)` feeds `m`.
CREATE TABLE ttl_nondet_sort_key
(
    g UInt32,
    x UInt32,
    ts DateTime,
    m DateTime MATERIALIZED now() + toIntervalSecond(x),
    saved_m DateTime DEFAULT m
)
ENGINE = MergeTree PRIMARY KEY g ORDER BY (g, m)
TTL ts + toIntervalDay(1) GROUP BY g SET x = max(x)
SETTINGS min_bytes_for_wide_part = 0, ttl_resort_max_bytes_before_external_sort = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE ttl_det_sort_key
(
    g UInt32,
    x UInt32,
    ts DateTime,
    m DateTime MATERIALIZED toDateTime('2000-01-01') + toIntervalSecond(x),
    saved_m DateTime DEFAULT m
)
ENGINE = MergeTree PRIMARY KEY g ORDER BY (g, m)
TTL ts + toIntervalDay(1) GROUP BY g SET x = max(x)
SETTINGS min_bytes_for_wide_part = 0, ttl_resort_max_bytes_before_external_sort = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- The `GROUP BY`-key path: a second TTL groups by `m`, which an earlier firing `SET` feeds.
CREATE TABLE ttl_nondet_group_by_key
(
    g UInt32,
    x UInt32,
    v UInt32,
    ts DateTime,
    m DateTime MATERIALIZED now() + toIntervalSecond(x)
)
ENGINE = MergeTree ORDER BY (g, m)
TTL ts + toIntervalDay(1) GROUP BY g SET x = max(x),
    ts + toIntervalDay(1) GROUP BY g, m SET v = max(v)
SETTINGS min_bytes_for_wide_part = 0, ttl_group_by_unsorted_max_bytes_before_external_group_by = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE ttl_det_group_by_key
(
    g UInt32,
    x UInt32,
    v UInt32,
    ts DateTime,
    m DateTime MATERIALIZED toDateTime('2000-01-01') + toIntervalSecond(x)
)
ENGINE = MergeTree ORDER BY (g, m)
TTL ts + toIntervalDay(1) GROUP BY g SET x = max(x),
    ts + toIntervalDay(1) GROUP BY g, m SET v = max(v)
SETTINGS min_bytes_for_wide_part = 0, ttl_group_by_unsorted_max_bytes_before_external_group_by = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Two parts per table, so the merge below is a real multi-part TTL merge.
INSERT INTO ttl_nondet_sort_key (g, x, ts) SELECT number % 1000, number, now() - toIntervalDay(2) FROM numbers(2000);
INSERT INTO ttl_nondet_sort_key (g, x, ts) SELECT number % 1000, number, now() - toIntervalDay(2) FROM numbers(2000, 2000);
INSERT INTO ttl_det_sort_key (g, x, ts) SELECT number % 1000, number, now() - toIntervalDay(2) FROM numbers(2000);
INSERT INTO ttl_det_sort_key (g, x, ts) SELECT number % 1000, number, now() - toIntervalDay(2) FROM numbers(2000, 2000);
INSERT INTO ttl_nondet_group_by_key (g, x, v, ts) SELECT number % 1000, number, number, now() - toIntervalDay(2) FROM numbers(2000);
INSERT INTO ttl_nondet_group_by_key (g, x, v, ts) SELECT number % 1000, number, number, now() - toIntervalDay(2) FROM numbers(2000, 2000);
INSERT INTO ttl_det_group_by_key (g, x, v, ts) SELECT number % 1000, number, number, now() - toIntervalDay(2) FROM numbers(2000);
INSERT INTO ttl_det_group_by_key (g, x, v, ts) SELECT number % 1000, number, number, now() - toIntervalDay(2) FROM numbers(2000, 2000);

-- `optimize_throw_if_noop` turns a merge that was not assigned into an error: `OPTIMIZE` otherwise
-- reports success on a table it left untouched, and the oracles below would read the pre-merge parts.
OPTIMIZE TABLE ttl_nondet_sort_key FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE ttl_det_sort_key FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE ttl_nondet_group_by_key FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE ttl_det_group_by_key FINAL SETTINGS optimize_throw_if_noop = 1;

-- The stored value: preserved for the non-deterministic default, recomputed for the deterministic one.
SELECT 'stored m preserved', countIf(m != saved_m) = 0 FROM ttl_nondet_sort_key;
SELECT 'stored m recomputed', countIf(m != saved_m) > 0 FROM ttl_det_sort_key;

-- The data is correct in every arm: one row per group, holding the aggregated value of `x`.
SELECT 'rows', count(), countIf(x = 3000 + g) FROM ttl_nondet_sort_key;
SELECT 'rows', count(), countIf(x = 3000 + g) FROM ttl_det_sort_key;
SELECT 'rows', count(), countIf(x = 3000 + g) FROM ttl_nondet_group_by_key;
SELECT 'rows', count(), countIf(x = 3000 + g) FROM ttl_det_group_by_key;
CHECK TABLE ttl_nondet_sort_key SETTINGS check_query_single_value_result = 1;
CHECK TABLE ttl_nondet_group_by_key SETTINGS check_query_single_value_result = 1;

SYSTEM FLUSH LOGS part_log;

-- The repairs a non-deterministic default must NOT pay for, and the controls that show they fire.
SELECT 'resort', sum(ProfileEvents['ExternalSortWritePart']) = 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_nondet_sort_key' AND event_type = 'MergeParts';
SELECT 'resort control', sum(ProfileEvents['ExternalSortWritePart']) > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_det_sort_key' AND event_type = 'MergeParts';
SELECT 'unsorted aggregation', sum(ProfileEvents['ExternalAggregationWritePart']) = 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_nondet_group_by_key' AND event_type = 'MergeParts';
SELECT 'unsorted aggregation control', sum(ProfileEvents['ExternalAggregationWritePart']) > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_det_group_by_key' AND event_type = 'MergeParts';

DROP TABLE ttl_nondet_sort_key;
DROP TABLE ttl_det_sort_key;
DROP TABLE ttl_nondet_group_by_key;
DROP TABLE ttl_det_group_by_key;
