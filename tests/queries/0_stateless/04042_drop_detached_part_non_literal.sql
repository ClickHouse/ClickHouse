-- PR #99489 added a test for `ALTER TABLE DROP PART` with a non-String typed
-- query parameter. This test covers the same fix in `dropDetached`, which uses
-- `getPartNameFromAST` at `MergeTreeData.cpp` line 7706. A non-String typed
-- parameter (`{partition:Date}`) is replaced by `_CAST(...)` — an `ASTFunction`,
-- not `ASTLiteral` — so the old code threw `LOGICAL_ERROR`. The fix throws
-- `BAD_ARGUMENTS` instead.

DROP TABLE IF EXISTS t_04042_dda;
CREATE TABLE t_04042_dda (x UInt8) ENGINE = MergeTree ORDER BY x;

SET param_partition = '2024-01-01';
ALTER TABLE t_04042_dda DROP DETACHED PART {partition:Date} SETTINGS allow_drop_detached = 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_04042_dda;
