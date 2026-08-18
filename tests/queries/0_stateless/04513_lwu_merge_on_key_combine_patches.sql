-- Tests for combining several MergeOnKey patches in one merge pass.
-- Patch parts that update the same set of columns are combined into one multi-source
-- patch; when several of them update the same row, the highest data version must win.

SET enable_lightweight_update = 1;
SET insert_keeper_fault_injection_probability = 0.0;

DROP TABLE IF EXISTS t_lwu_combine;

CREATE TABLE t_lwu_combine (k UInt64, a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

SYSTEM STOP MERGES t_lwu_combine;

INSERT INTO t_lwu_combine SELECT number, 0, 0 FROM numbers(1000);

-- The same column set -> one combine group with three patches.
-- The ranges overlap and the latest update must win on every row.
UPDATE t_lwu_combine SET a = 1 WHERE k < 600;
UPDATE t_lwu_combine SET a = 2 WHERE k >= 400;
UPDATE t_lwu_combine SET a = 3 WHERE k >= 500 AND k < 550;

SELECT a, count() FROM t_lwu_combine GROUP BY a ORDER BY a;

-- Different column sets -> different combine groups.
-- Conflicts across groups are resolved by row versions at application time.
UPDATE t_lwu_combine SET b = 10 WHERE k < 100;
UPDATE t_lwu_combine SET a = 4, b = 20 WHERE k < 50;

SELECT a, b, count() FROM t_lwu_combine WHERE k < 100 GROUP BY a, b ORDER BY a, b;

DROP TABLE t_lwu_combine;

-- The same with duplicate sort keys, so that equal-key runs contain many rows
-- and conflicts between patches are resolved through the hash map path.
CREATE TABLE t_lwu_combine (k UInt64, a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

SYSTEM STOP MERGES t_lwu_combine;

INSERT INTO t_lwu_combine SELECT intDiv(number, 100), number, 0 FROM numbers(1000);

UPDATE t_lwu_combine SET b = 1 WHERE a % 2 = 0;
UPDATE t_lwu_combine SET b = 2 WHERE a % 4 = 0;

SELECT b, count() FROM t_lwu_combine GROUP BY b ORDER BY b;

DROP TABLE t_lwu_combine;
