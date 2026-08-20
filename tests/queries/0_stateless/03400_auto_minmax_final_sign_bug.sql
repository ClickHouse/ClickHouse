-- Reproduces a bug where mixed PK and non-PK minmax skip indexes break FINAL queries.
--
-- The minmax index on `sign` evaluates the `sign = 1` filter and drops the part
-- containing the delete marker (sign = -1). The recovery mechanism
-- (`findPKRangesForFinalAfterSkipIndex`) is disabled because the minmax index on
-- `key` (a PK column) is also useful, causing `areSkipIndexColumnsInPrimaryKey`
-- to return true.

DROP TABLE IF EXISTS t_final_minmax;

CREATE TABLE t_final_minmax
(
    key Int32,
    value String,
    sign Int8,
    ver UInt64,
    INDEX idx_key key TYPE minmax,
    INDEX idx_sign sign TYPE minmax
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY key
ORDER BY key;

SYSTEM STOP MERGES t_final_minmax;

-- Part 1: original row
INSERT INTO t_final_minmax VALUES (2, 'original', 1, 1);

-- Part 2: delete marker (higher version, sign = -1)
INSERT INTO t_final_minmax VALUES (2, '', -1, 2);

-- FINAL should keep only the row with highest version (ver=2, sign=-1).
-- Then sign = 1 filter should exclude it, returning empty result.
-- Bug: idx_sign drops the delete-marker part, and the recovery is disabled
-- because idx_key (on a PK column) is also useful.
SELECT value FROM t_final_minmax FINAL WHERE key = 2 AND sign = 1;

DROP TABLE t_final_minmax;
