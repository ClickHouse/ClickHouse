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

DROP TABLE IF EXISTS ttl_where_stopped;

-- While TTL merges are stopped the merge must not delete anything, but it must not inherit the
-- source parts' rows-WHERE info either: that info says nothing about the value the merge produces,
-- and a part claiming it has nothing to expire would never be picked for a TTL merge again.
-- The recompression TTL is here to check that refreshing the rows-WHERE info leaves the other TTL
-- kinds alone - and it is the strictest choice, because `checkAllTTLCalculated` would not restore
-- a lost recompression info through a later merge.
CREATE TABLE ttl_where_stopped
(
    key UInt64,
    occurrences SimpleAggregateFunction(sum, Int64),
    expiry SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY key
TTL expiry DELETE WHERE occurrences = 0,
    expiry + INTERVAL 1 YEAR RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP TTL MERGES ttl_where_stopped;

INSERT INTO ttl_where_stopped VALUES (1, -1, '2020-01-01 00:00:00');
INSERT INTO ttl_where_stopped VALUES (1, +1, '2020-01-01 00:00:00');

OPTIMIZE TABLE ttl_where_stopped FINAL;

-- The row is still here, and the merged part now reports it as expirable.
SELECT 'stopped', key, occurrences, expiry FROM ttl_where_stopped ORDER BY key;
SELECT 'stopped ttl info', rows_where_ttl_info.min, rows_where_ttl_info.max, recompression_ttl_info.min
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_where_stopped' AND active;

-- The background side of this - that TTL selection actually picks the part up once TTL merges are
-- allowed again - is covered by 04652_ttl_where_merge_combined_values_background, which polls
-- instead of forcing a merge.
DROP TABLE ttl_where_stopped;

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
