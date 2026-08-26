-- A `TTL ... DELETE WHERE` must be evaluated against the values a merge produces, not only against
-- the values the source rows were written with. On AggregatingMergeTree / SummingMergeTree the merge
-- combines rows, so the condition can first become true in the merge output.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ttl_where_aggregating;

CREATE TABLE ttl_where_aggregating
(
    key UInt64,
    occurrences SimpleAggregateFunction(sum, Int64),
    expiry SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY key
TTL expiry DELETE WHERE occurrences = 0
SETTINGS min_bytes_for_wide_part = 0;

-- Keep the parts apart until the single OPTIMIZE below, so the merge is deterministic:
-- a background merge of a subset of the parts would change which rows are combined.
SYSTEM STOP MERGES ttl_where_aggregating;

-- key 1: sums to 0 and is expired -> must be deleted (this is the bug).
INSERT INTO ttl_where_aggregating VALUES (1, -1, '2020-01-01 00:00:00');
INSERT INTO ttl_where_aggregating VALUES (1, +1, '2020-01-01 00:00:00');
-- key 2: sums to 0 but is not expired yet -> must survive.
INSERT INTO ttl_where_aggregating VALUES (2, -1, '2106-01-01 00:00:00');
INSERT INTO ttl_where_aggregating VALUES (2, +1, '2106-01-01 00:00:00');
-- key 3: expired but does not satisfy the WHERE -> must survive.
INSERT INTO ttl_where_aggregating VALUES (3, 5, '2020-01-01 00:00:00');

SYSTEM START MERGES ttl_where_aggregating;
OPTIMIZE TABLE ttl_where_aggregating FINAL;

SELECT 'aggregating', key, occurrences, expiry FROM ttl_where_aggregating ORDER BY key;

DROP TABLE ttl_where_aggregating;

DROP TABLE IF EXISTS ttl_where_summing;

-- `weight` keeps the summed row alive: SummingMergeTree drops a row whose every summed column is
-- zero, so `occurrences` alone could never be observed as 0.
CREATE TABLE ttl_where_summing
(
    key UInt64,
    occurrences Int64,
    weight Int64,
    expiry DateTime
)
ENGINE = SummingMergeTree((occurrences, weight))
ORDER BY (key, expiry)
TTL expiry DELETE WHERE occurrences = 0
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES ttl_where_summing;

INSERT INTO ttl_where_summing VALUES (1, -1, 1, '2020-01-01 00:00:00');
INSERT INTO ttl_where_summing VALUES (1, +1, 1, '2020-01-01 00:00:00');
INSERT INTO ttl_where_summing VALUES (2, -1, 1, '2106-01-01 00:00:00');
INSERT INTO ttl_where_summing VALUES (2, +1, 1, '2106-01-01 00:00:00');
INSERT INTO ttl_where_summing VALUES (3, 5, 1, '2020-01-01 00:00:00');

SYSTEM START MERGES ttl_where_summing;
OPTIMIZE TABLE ttl_where_summing FINAL;

SELECT 'summing', key, occurrences, weight, expiry FROM ttl_where_summing ORDER BY key;

DROP TABLE ttl_where_summing;

DROP TABLE IF EXISTS ttl_where_coalescing;

-- Coalescing takes the first non-NULL value per column, so a combination of `a` and `b` that no
-- single source row had can only appear in the merge output.
CREATE TABLE ttl_where_coalescing
(
    key UInt64,
    a Nullable(Int64),
    b Nullable(Int64),
    expiry DateTime
)
ENGINE = CoalescingMergeTree
ORDER BY (key, expiry)
TTL expiry DELETE WHERE a = 1 AND b = 2
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES ttl_where_coalescing;

INSERT INTO ttl_where_coalescing VALUES (1, 1, NULL, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (1, NULL, 2, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (2, 1, NULL, '2106-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (2, NULL, 2, '2106-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (3, 1, NULL, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (3, NULL, 9, '2020-01-01 00:00:00');

SYSTEM START MERGES ttl_where_coalescing;
OPTIMIZE TABLE ttl_where_coalescing FINAL;

SELECT 'coalescing', key, a, b, expiry FROM ttl_where_coalescing ORDER BY key;

DROP TABLE ttl_where_coalescing;

-- `GraphiteMergeTree` rolls up the rows of a time bucket into one, so the rolled-up `Value` is a
-- value no source row had. `graphite_rollup` (from the stateless config) sums `Value` for a `Path`
-- matching `sum`, so two counters that cancel out roll up to zero. Rollup only aggregates rows that
-- arrive together in one block, so the pairs go in with a single INSERT - and with
-- `optimize_on_insert = 0`, because otherwise the INSERT itself rolls them up and applies the TTL,
-- and the merge this test is about never sees the pair.
DROP TABLE IF EXISTS ttl_where_graphite;

-- Rollup groups by `Path` and by `Time` rounded within the day, so `Path` separates the cases and
-- the two rows of a pair are 60 s apart - distinct rows to aggregate, one 6000 s bucket (their age
-- is past the pattern's 2-day boundary).
CREATE TABLE ttl_where_graphite
(
    Path String,
    Time DateTime('UTC'),
    Value Float64,
    Version UInt32,
    expiry DateTime
)
ENGINE = GraphiteMergeTree('graphite_rollup')
ORDER BY (Path, Time)
TTL expiry DELETE WHERE Value = 0
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_where_graphite SETTINGS optimize_on_insert = 0 VALUES
    -- sum_1: rolls up to Value = 0 and is expired -> must be deleted.
    ('sum_1', '2020-01-01 00:00:00', -1, 1, '2020-01-01 00:00:00'),
    ('sum_1', '2020-01-01 00:01:00', +1, 1, '2020-01-01 00:00:00'),
    -- sum_2: rolls up to Value = 0 but is not expired yet -> must survive.
    ('sum_2', '2020-01-01 00:00:00', -1, 1, '2106-01-01 00:00:00'),
    ('sum_2', '2020-01-01 00:01:00', +1, 1, '2106-01-01 00:00:00'),
    -- sum_3: expired but does not satisfy the WHERE -> must survive.
    ('sum_3', '2020-01-01 00:00:00', 5, 1, '2020-01-01 00:00:00');

OPTIMIZE TABLE ttl_where_graphite FINAL;

SELECT 'graphite', Path, Value, expiry FROM ttl_where_graphite ORDER BY Path;

DROP TABLE ttl_where_graphite;

-- `SYSTEM STOP TTL MERGES` must still suppress the deletion. The `TTLStep` is added for a reason
-- other than an expired TTL here - `c` is absent from every source part and has no default, so it
-- counts as an expired column - which is what makes the blocker's clearing of the forced flag
-- load-bearing rather than decorative.
DROP TABLE IF EXISTS ttl_where_blocked;

CREATE TABLE ttl_where_blocked
(
    key UInt64,
    occurrences SimpleAggregateFunction(sum, Int64),
    expiry SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY key
TTL expiry DELETE WHERE occurrences = 0
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES ttl_where_blocked;

INSERT INTO ttl_where_blocked VALUES (1, -1, '2020-01-01 00:00:00');
INSERT INTO ttl_where_blocked VALUES (1, +1, '2020-01-01 00:00:00');

ALTER TABLE ttl_where_blocked ADD COLUMN c String;

SYSTEM STOP TTL MERGES ttl_where_blocked;
SYSTEM START MERGES ttl_where_blocked;
OPTIMIZE TABLE ttl_where_blocked FINAL;

-- The merge combined the rows into a match, but TTL merges are stopped, so nothing may be deleted.
SELECT 'blocked', key, occurrences FROM ttl_where_blocked ORDER BY key;

-- ... and the deletion is only delayed, not lost.
SYSTEM START TTL MERGES ttl_where_blocked;
OPTIMIZE TABLE ttl_where_blocked FINAL;
SELECT 'unblocked', count() FROM ttl_where_blocked;

DROP TABLE ttl_where_blocked;

-- Patch parts are the other way a merge output holds a value no source row had, and they do it on
-- any merge mode: a lightweight `UPDATE` writes a patch part, the merge reads the patched value, and
-- the part's `rows_where_ttl` was seeded from the base parts only. `Ordinary` here, so the mode gate
-- above cannot be what saves it.
DROP TABLE IF EXISTS ttl_where_patched;

CREATE TABLE ttl_where_patched
(
    id UInt64,
    val UInt8,
    expiry DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL expiry DELETE WHERE val = 0
SETTINGS min_bytes_for_wide_part = 0, apply_patches_on_merge = 1,
         enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES ttl_where_patched;

-- id 1: the patch makes it match the WHERE, and it is expired -> must be deleted.
INSERT INTO ttl_where_patched VALUES (1, 5, '2020-01-01 00:00:00');
-- id 2: the patch makes it match too, but it is not expired -> must survive.
INSERT INTO ttl_where_patched VALUES (2, 5, '2106-01-01 00:00:00');
-- id 3: expired, and the patch leaves it non-matching -> must survive.
INSERT INTO ttl_where_patched VALUES (3, 7, '2020-01-01 00:00:00');

UPDATE ttl_where_patched SET val = 0 WHERE id IN (1, 2)
SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

SYSTEM START MERGES ttl_where_patched;
OPTIMIZE TABLE ttl_where_patched FINAL;

SELECT 'patched', id, val, expiry FROM ttl_where_patched ORDER BY id;

DROP TABLE ttl_where_patched;

-- The same on the vertical algorithm, which stays eligible for `Ordinary`: there the rows are
-- filtered before the merge by `TTLDeleteFilterTransform`, which skips a rows-WHERE TTL whose source
-- info reports nothing expirable unless it is forced. The thresholds and part format are pinned so
-- the algorithm cannot silently fall back to Horizontal and test the path above twice - the last
-- assertion checks that it really was Vertical.
DROP TABLE IF EXISTS ttl_where_patched_vertical;

CREATE TABLE ttl_where_patched_vertical
(
    id UInt64,
    val UInt8,
    expiry DateTime,
    c1 UInt64,
    c2 String
)
ENGINE = MergeTree
ORDER BY id
TTL expiry DELETE WHERE val = 0
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, apply_patches_on_merge = 1,
         enable_block_number_column = 1, enable_block_offset_column = 1,
         enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES ttl_where_patched_vertical;

INSERT INTO ttl_where_patched_vertical
    SELECT number, 5, toDateTime('2020-01-01 00:00:00'), number, 'x' FROM numbers(1000);
INSERT INTO ttl_where_patched_vertical
    SELECT number + 1000, 7, toDateTime('2020-01-01 00:00:00'), number, 'y' FROM numbers(1000);

UPDATE ttl_where_patched_vertical SET val = 0 WHERE id < 10
SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

SYSTEM START MERGES ttl_where_patched_vertical;
OPTIMIZE TABLE ttl_where_patched_vertical FINAL;

SELECT 'patched vertical', count(), countIf(val = 0) FROM ttl_where_patched_vertical;

SYSTEM FLUSH LOGS part_log;
SELECT 'patched vertical algorithm', groupUniqArray(merge_algorithm) FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase()
  AND table = 'ttl_where_patched_vertical' AND event_type = 'MergeParts';

DROP TABLE ttl_where_patched_vertical;

-- An Ordinary merge never changes a column value, so a row that does not satisfy the WHERE must
-- survive however many times it is merged, and the merged part must keep reporting nothing
-- expirable. This is the guarantee added by https://github.com/ClickHouse/ClickHouse/pull/86965.
DROP TABLE IF EXISTS ttl_where_ordinary;

CREATE TABLE ttl_where_ordinary
(
    key UInt64,
    occurrences Int64,
    expiry DateTime
)
ENGINE = MergeTree
ORDER BY key
TTL expiry DELETE WHERE occurrences = 0
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_where_ordinary VALUES (1, 5, '2020-01-01 00:00:00');
INSERT INTO ttl_where_ordinary VALUES (2, 5, '2020-01-01 00:00:00');

OPTIMIZE TABLE ttl_where_ordinary FINAL;

SELECT 'ordinary', key, occurrences, expiry FROM ttl_where_ordinary ORDER BY key;
SELECT 'ordinary ttl info', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_where_ordinary' AND active;

DROP TABLE ttl_where_ordinary;
