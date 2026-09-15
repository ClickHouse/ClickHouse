-- A `TTL ... DELETE WHERE` must be evaluated against the values a merge produces, not only against
-- the values the source rows were written with. `SYSTEM STOP TTL MERGES` must still suppress the
-- deletion, even when the TTL step is added for an unrelated reason.
--
-- Split across several tests of the same number so no single one runs long on the slower CI
-- configurations: `_combined_values`, `_coalescing_and_graphite`, `_patch_parts`, `_background`.

SET session_timezone = 'UTC';

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
