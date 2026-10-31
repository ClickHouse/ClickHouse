-- A GROUP BY TTL can reduce rows after the rows-TTL bounds have been collected. Once the GROUP BY
-- TTL is removed, a later shiftable MODIFY TTL must recalculate those bounds instead of shifting the
-- stale ones left by the previous merge.
SET alter_sync = 2;
SET allow_suspicious_ttl_expressions = 1;

DROP TABLE IF EXISTS materialize_ttl_group_by_provenance;
CREATE TABLE materialize_ttl_group_by_provenance
(
    id UInt8,
    d DateTime('UTC'),
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 300 DAY,
    d + INTERVAL 1 DAY GROUP BY id SET d = max(d), value = sum(value);

SYSTEM STOP TTL MERGES materialize_ttl_group_by_provenance;
INSERT INTO materialize_ttl_group_by_provenance VALUES (1, now('UTC') - INTERVAL 100 DAY, 1), (1, now('UTC') - INTERVAL 10 DAY, 1);
OPTIMIZE TABLE materialize_ttl_group_by_provenance FINAL;

-- Remove the GROUP BY TTL without materializing. The following merge clears its TTL metadata but
-- does not recalculate the unexpired rows TTL, whose old minimum is 90 days behind the aggregated row.
ALTER TABLE materialize_ttl_group_by_provenance MODIFY TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE materialize_ttl_group_by_provenance FINAL;
ALTER TABLE materialize_ttl_group_by_provenance MODIFY TTL d + INTERVAL 50 DAY;

SELECT delete_ttl_info_min > now('UTC') + INTERVAL 30 DAY FROM system.parts
WHERE database = currentDatabase() AND table = 'materialize_ttl_group_by_provenance' AND active;

SYSTEM START TTL MERGES materialize_ttl_group_by_provenance;
DROP TABLE materialize_ttl_group_by_provenance;
