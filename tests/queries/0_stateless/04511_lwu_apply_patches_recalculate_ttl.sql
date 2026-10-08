-- Test: APPLY PATCHES must recalculate a column TTL whose expression depends on
-- a patch-updated column, just like a classical ALTER UPDATE.
DROP TABLE IF EXISTS t_lwu_ttl_patch;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_ttl_patch;
CREATE TABLE t_lwu_ttl_patch
(
    id UInt64,
    d DateTime,
    v UInt64 TTL d + INTERVAL 1 YEAR
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

-- d starts far in the future so the column TTL (d + 1 YEAR) is not expired.
INSERT INTO t_lwu_ttl_patch SELECT number, '2099-01-01 00:00:00', 100 FROM numbers(10);

-- Move d far into the past for half the rows so their column TTL expires.
UPDATE t_lwu_ttl_patch SET d = '2000-01-01 00:00:00' WHERE id < 5;
ALTER TABLE t_lwu_ttl_patch APPLY PATCHES SETTINGS mutations_sync = 2;

-- Rows with the patched (expired) d must have v reset to the type default (0);
-- the rest keep v = 100. Stale TTL would leave all v = 100.
SELECT id, v FROM t_lwu_ttl_patch ORDER BY id;

DROP TABLE t_lwu_ttl_patch;
