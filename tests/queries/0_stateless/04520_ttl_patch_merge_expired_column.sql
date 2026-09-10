-- The blocked TTL recalculation on a patch merge must survive a column TTL whose input column is
-- already fully expired and physically dropped from the part.

SET allow_experimental_lightweight_update = 1;
SET mutations_sync = 2;
SET session_timezone = 'UTC'; -- the reference renders an epoch-zero DateTime

DROP TABLE IF EXISTS t_ttl_patch_expired SYNC;

-- x's rule references itself (the original crash shape); x2's references only d, so its rule
-- stays evaluable while x2 is gone (the silent-metadata shape).
CREATE TABLE t_ttl_patch_expired (d DateTime, x DateTime TTL x + INTERVAL 1 SECOND, x2 Int32 TTL d + INTERVAL 1 SECOND, y Int32)
ENGINE = MergeTree ORDER BY tuple()
-- A table-level rule reading the expired column: must evaluate over defaults, not fail the merge.
-- 50 years keeps the real bound inside DateTime's range; 100 would wrap past and delete the rows.
TTL x + INTERVAL 50 YEAR
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_ttl_patch_expired VALUES (now() - INTERVAL 1 HOUR, now() - INTERVAL 1 HOUR, 1, 1);

-- The column TTL is long past due, so this merge drops `x` from the part physically.
OPTIMIZE TABLE t_ttl_patch_expired FINAL;
SELECT 'expired columns still stored', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_patch_expired' AND active AND column IN ('x', 'x2');

SYSTEM STOP TTL MERGES t_ttl_patch_expired;

UPDATE t_ttl_patch_expired SET y = 2 WHERE TRUE;

-- The merge carries a patch while TTL removal is blocked: the recalculation branch must skip the
-- rule whose input column is gone instead of failing the whole merge on it.
OPTIMIZE TABLE t_ttl_patch_expired FINAL;

SELECT 'after blocked patch merge', x, x2, y FROM t_ttl_patch_expired;

SYSTEM START TTL MERGES t_ttl_patch_expired;
DROP TABLE t_ttl_patch_expired;

-- The blocked patch merge must reach the TTL recalculation step even when an expired column rides
-- along, and rules sharing a time expression must merge their shared rows-WHERE slot, not overwrite.
DROP TABLE IF EXISTS t_ttl_patch_rows_where SYNC;

CREATE TABLE t_ttl_patch_rows_where (ts DateTime, flag UInt8, flag2 UInt8, x DateTime TTL x + INTERVAL 1 SECOND, y Int32)
ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 50 YEAR DELETE WHERE flag = 1, ts + INTERVAL 50 YEAR DELETE WHERE flag2 = 1
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

-- One part; x expired an hour ago but was never merged out, so this part still stores it and the
-- patch merge below classifies it expired-unread, which used to divert the recalculation step.
INSERT INTO t_ttl_patch_rows_where VALUES
    (toDateTime('2001-01-01 00:00:00'), 1, 0, now() - INTERVAL 1 HOUR, 1),
    (toDateTime('2002-01-01 00:00:00'), 0, 1, now() - INTERVAL 1 HOUR, 1);

SYSTEM STOP TTL MERGES t_ttl_patch_rows_where;

UPDATE t_ttl_patch_rows_where SET y = 2 WHERE TRUE;

OPTIMIZE TABLE t_ttl_patch_rows_where FINAL;

-- One shared slot spanning both rules: min from the flag rule, max from the flag2 rule.
SELECT 'rows-where bounds survive the blocked patch merge',
       length(rows_where_ttl_info.expression),
       rows_where_ttl_info.min[1] = toDateTime('2051-01-01 00:00:00'),
       rows_where_ttl_info.max[1] = toDateTime('2052-01-01 00:00:00')
FROM system.parts
-- The patch part is active alongside the merged part and carries no TTL infos of its own.
WHERE database = currentDatabase() AND table = 't_ttl_patch_rows_where' AND active AND partition_id NOT LIKE 'patch-%';

SELECT 'rows kept while blocked', count() FROM t_ttl_patch_rows_where WHERE y = 2;

SYSTEM START TTL MERGES t_ttl_patch_rows_where;
DROP TABLE t_ttl_patch_rows_where;

-- An index reading the expired column keeps it in the merge header (MergeTask's
-- merge_required_columns), so the preserve decision cannot key on header presence.
DROP TABLE IF EXISTS t_ttl_patch_indexed SYNC;

-- x carries a DEFAULT far in the past, and a recompression rule reads x: if the merge saw x's
-- stale pre-expiry values the bound would land a year from now instead.
CREATE TABLE t_ttl_patch_indexed (d DateTime, x DateTime DEFAULT toDateTime('2000-01-01 00:00:00') TTL x + INTERVAL 1 SECOND, y Int32,
    INDEX idx_x (x, y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
TTL x + INTERVAL 1 YEAR RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_ttl_patch_indexed VALUES (now() - INTERVAL 1 HOUR, now() - INTERVAL 1 HOUR, 1);

OPTIMIZE TABLE t_ttl_patch_indexed FINAL;

SYSTEM STOP TTL MERGES t_ttl_patch_indexed;
UPDATE t_ttl_patch_indexed SET y = 2 WHERE TRUE;
OPTIMIZE TABLE t_ttl_patch_indexed FINAL;

-- The merge has to complete: the recalculation step re-adds the columns it expired, and the index
-- reading x is built after it, so a narrower stream there fails the whole merge.
SELECT 'indexed expired column merge completed', count(), min(y) FROM t_ttl_patch_indexed;

SELECT 'recompression bound built from the default', countIf(recompression_ttl_info.max[1] < now())
FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_patch_indexed' AND active AND partition_id NOT LIKE 'patch-%';

SYSTEM START TTL MERGES t_ttl_patch_indexed;
DROP TABLE t_ttl_patch_indexed;
