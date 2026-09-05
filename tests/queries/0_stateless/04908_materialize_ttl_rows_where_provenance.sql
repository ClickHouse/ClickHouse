-- A ROWS WHERE TTL can remove rows after the rows-TTL bounds have been collected. Once the
-- conditional TTL is removed, a later shiftable MODIFY TTL must recalculate those bounds instead
-- of shifting the stale ones left by the previous merge.
SET alter_sync = 2;

DROP TABLE IF EXISTS materialize_ttl_rows_where_provenance;
CREATE TABLE materialize_ttl_rows_where_provenance
(
    id UInt8,
    d DateTime('UTC'),
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 300 DAY,
    d + INTERVAL 1 DAY WHERE id = 1;

SYSTEM STOP TTL MERGES materialize_ttl_rows_where_provenance;
INSERT INTO materialize_ttl_rows_where_provenance VALUES (1, now('UTC') - INTERVAL 100 DAY, 1), (2, now('UTC') - INTERVAL 10 DAY, 1);
OPTIMIZE TABLE materialize_ttl_rows_where_provenance FINAL;

-- Remove the conditional TTL without materializing. The following merge clears its TTL metadata
-- but does not recalculate the unexpired rows TTL, whose old minimum is 90 days behind the row
-- left by the conditional delete.
ALTER TABLE materialize_ttl_rows_where_provenance MODIFY TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE materialize_ttl_rows_where_provenance FINAL;
ALTER TABLE materialize_ttl_rows_where_provenance MODIFY TTL d + INTERVAL 50 DAY;

SELECT delete_ttl_info_min > now('UTC') + INTERVAL 30 DAY FROM system.parts
WHERE database = currentDatabase() AND table = 'materialize_ttl_rows_where_provenance' AND active;

SYSTEM START TTL MERGES materialize_ttl_rows_where_provenance;
DROP TABLE materialize_ttl_rows_where_provenance;
