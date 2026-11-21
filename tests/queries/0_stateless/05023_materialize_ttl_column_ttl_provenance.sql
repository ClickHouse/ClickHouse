-- A column TTL resets its column to the default value after `TTLDeleteAlgorithm` has already collected
-- the rows-TTL bounds of the part from the original values. Once that column TTL is removed and a merge
-- clears its metadata, a later shiftable MODIFY TTL must recalculate those bounds instead of shifting the
-- stale ones - otherwise it keeps rows the regular rewrite deletes.
SET alter_sync = 2;
SET allow_suspicious_ttl_expressions = 1;

DROP TABLE IF EXISTS materialize_ttl_column_ttl_provenance;
CREATE TABLE materialize_ttl_column_ttl_provenance
(
    id UInt8,
    d DateTime('UTC') TTL d + INTERVAL 1 DAY,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0;

INSERT INTO materialize_ttl_column_ttl_provenance VALUES (1, now('UTC') - INTERVAL 100 DAY, 1), (2, now('UTC'), 2);
-- The merge resets `d` of the first row to the default (the epoch) because its column TTL expired, while
-- the rows TTL is not expired and its bounds - computed from the original `d` - are only propagated.
OPTIMIZE TABLE materialize_ttl_column_ttl_provenance FINAL;

-- A part whose column TTL is expired but not yet applied, so that the merge below is a TTL merge even
-- though the column TTL is gone from the metadata by then.
SYSTEM STOP MERGES materialize_ttl_column_ttl_provenance;
INSERT INTO materialize_ttl_column_ttl_provenance VALUES (3, now('UTC') - INTERVAL 100 DAY, 3);
ALTER TABLE materialize_ttl_column_ttl_provenance MODIFY COLUMN d REMOVE TTL SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES materialize_ttl_column_ttl_provenance;
-- Clears the column TTL metadata, leaving a part whose only TTL info is the rows TTL.
OPTIMIZE TABLE materialize_ttl_column_ttl_provenance FINAL;

-- Shortening the interval leaves the shifted bounds unexpired, which is exactly when the metadata-only
-- fast path applies. The first row must still be deleted: its `d` was reset to the epoch.
ALTER TABLE materialize_ttl_column_ttl_provenance MODIFY TTL d + INTERVAL 250 DAY;
SELECT count(), min(id) FROM materialize_ttl_column_ttl_provenance;

DROP TABLE materialize_ttl_column_ttl_provenance;
