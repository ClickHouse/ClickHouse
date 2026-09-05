-- A `TTL ... DELETE WHERE` must be evaluated against the values a merge produces, not only against
-- the values the source rows were written with. Here the merge combines rows, so the condition can
-- first become true in the merge output - plus the `Ordinary` control, where it must not.
--
-- Split across several tests of the same number so no single one runs long on the slower CI
-- configurations: `_coalescing_and_graphite`, `_ttl_merges_stopped`, `_patch_parts`, `_background`.

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
