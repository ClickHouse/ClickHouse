-- Tests `MATERIALIZE STATISTICS` on a column that is not physically stored.

SET allow_statistics = 1;
-- The last check inspects statistics written by `INSERT`, so pin the setting that CI randomizes.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS tab;

-- A non-physical column with no statistics description is a wrong argument and is rejected when
-- the statement is issued, not silently skipped.
CREATE TABLE tab (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS auto_statistics_types = '';
ALTER TABLE tab MATERIALIZE STATISTICS b; -- { serverError ILLEGAL_STATISTICS }
DROP TABLE tab;

-- A column that does not exist at all is rejected the same way.
CREATE TABLE tab (a UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS auto_statistics_types = '';
ALTER TABLE tab MATERIALIZE STATISTICS nonexistent; -- { serverError ILLEGAL_STATISTICS }
DROP TABLE tab;

-- A mutation that named the column while it was still physical must drain, not retry forever.
-- Here the column carries only the implicit statistics that `auto_statistics_types` supplies, which
-- the same `ALTER` drops. The trailing synchronous mutation cannot complete until the queued one does.
CREATE TABLE tab (a UInt64 STATISTICS(tdigest), b UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab VALUES (1, 1);
SYSTEM STOP MERGES tab;
ALTER TABLE tab MATERIALIZE STATISTICS b SETTINGS mutations_sync = 0;
ALTER TABLE tab MODIFY COLUMN b UInt64 ALIAS a + 1 SETTINGS mutations_sync = 0;
SYSTEM START MERGES tab;
ALTER TABLE tab MATERIALIZE STATISTICS a SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'tab' AND NOT is_done;
DROP TABLE tab;

-- A physical column with statistics must keep building them.
CREATE TABLE tab (a UInt64, b UInt64 MATERIALIZED a * 2 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO tab (a) VALUES (1);
SELECT a, b FROM tab;
SELECT column, has(statistics, 'TDigest') FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY column;
DROP TABLE tab;
