-- Tests that DDL statements which clear / materialize statistics

SET mutations_sync = 1;
SET allow_statistics = 1;

DROP TABLE IF EXISTS tab;

-- tdigest is collected for numeric columns only, so `s` and `a` have no statistics.
CREATE TABLE tab
(
    f64           Float64,
    s             String,
    a             Array(Float64)
)
Engine = MergeTree()
ORDER BY tuple()
SETTINGS auto_statistics_types = 'tdigest';

-- Error case: Column does not exist
-- (relevant for CLEAR and MATERIALIZE)
ALTER TABLE tab CLEAR STATISTICS no_such_column; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab CLEAR STATISTICS IF EXISTS no_such_column; -- no-op
ALTER TABLE tab MATERIALIZE STATISTICS no_such_column; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MATERIALIZE STATISTICS IF EXISTS no_such_column; -- { serverError ILLEGAL_STATISTICS }

-- Column exists but has no statistics
ALTER TABLE tab CLEAR STATISTICS s; -- no-op
ALTER TABLE tab MATERIALIZE STATISTICS a; -- { serverError ILLEGAL_STATISTICS }

-- Finally, do a full-circle test of a good case.
INSERT INTO tab SETTINGS materialize_statistics_on_insert = 1 VALUES (1, 'x', [1]);
SELECT column, has(statistics, 'TDigest') FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('f64', 's', 'a') ORDER BY column;
ALTER TABLE tab CLEAR STATISTICS f64, s;
SELECT column, has(statistics, 'TDigest') FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('f64', 's', 'a') ORDER BY column;
ALTER TABLE tab MATERIALIZE STATISTICS f64;
SELECT column, has(statistics, 'TDigest') FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('f64', 's', 'a') ORDER BY column;

DROP TABLE tab;

-- Statistics that `auto_statistics_types` supplied are dropped on conversion, not refused.
CREATE TABLE tab (a UInt64, b UInt64) Engine = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = 'tdigest';
ALTER TABLE tab MODIFY COLUMN b UInt64 ALIAS a + 1;
INSERT INTO tab VALUES (1);
SELECT a, b FROM tab;
DROP TABLE tab;

-- A mutation that named the column while it was still physical must drain, not retry forever.
CREATE TABLE tab (a UInt64, b UInt64) Engine = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = 'tdigest';
INSERT INTO tab VALUES (1, 1);
SYSTEM STOP MERGES tab;
ALTER TABLE tab MATERIALIZE STATISTICS b SETTINGS mutations_sync = 0;
ALTER TABLE tab MODIFY COLUMN b UInt64 ALIAS a + 1 SETTINGS mutations_sync = 0;
SYSTEM START MERGES tab;
ALTER TABLE tab MATERIALIZE STATISTICS a SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'tab' AND NOT is_done;
DROP TABLE tab;

-- Naming a non-physical column with no statistics description is rejected, not silently skipped.
CREATE TABLE tab (a UInt64, b UInt64 ALIAS a + 1) Engine = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
ALTER TABLE tab MATERIALIZE STATISTICS b; -- { serverError ILLEGAL_STATISTICS }
DROP TABLE tab;
