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
TTL x + INTERVAL 100 YEAR
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
