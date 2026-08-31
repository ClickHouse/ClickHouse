-- The blocked TTL recalculation on a patch merge must survive a column TTL whose input column is
-- already fully expired and physically dropped from the part.

SET allow_experimental_lightweight_update = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_patch_expired SYNC;

CREATE TABLE t_ttl_patch_expired (d DateTime, x Int32 TTL d + INTERVAL 1 SECOND, y Int32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_patch_expired VALUES (now() - INTERVAL 1 HOUR, 1, 1);

-- The column TTL is long past due, so this merge drops `x` from the part physically.
OPTIMIZE TABLE t_ttl_patch_expired FINAL;
SELECT 'x still stored', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_patch_expired' AND active AND column = 'x';

SYSTEM STOP TTL MERGES t_ttl_patch_expired;

UPDATE t_ttl_patch_expired SET y = 2 WHERE TRUE;

-- The merge carries a patch while TTL removal is blocked: the recalculation branch must skip the
-- rule whose input column is gone instead of failing the whole merge on it.
OPTIMIZE TABLE t_ttl_patch_expired FINAL;

SELECT 'after blocked patch merge', x, y FROM t_ttl_patch_expired;

SYSTEM START TTL MERGES t_ttl_patch_expired;
DROP TABLE t_ttl_patch_expired;
