-- Tests `MATERIALIZE STATISTICS` on a column that is not physically stored.

SET allow_statistics = 1;

DROP TABLE IF EXISTS tab;

-- A grandfathered statistics description on a non-physical column: nothing can be built for it,
-- so the mutation is a logged no-op instead of a failure that would retry forever.
CREATE TABLE tab (a UInt64, b UInt64 ALIAS a + 1 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (1);
SELECT a, b FROM tab;
ALTER TABLE tab MATERIALIZE STATISTICS b SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'tab' AND NOT is_done;
DROP TABLE tab;

-- A non-physical column with no statistics description at all is a wrong argument and is rejected.
CREATE TABLE tab (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS auto_statistics_types = '';
ALTER TABLE tab MATERIALIZE STATISTICS b; -- { serverError ILLEGAL_STATISTICS }
DROP TABLE tab;

-- A column that does not exist at all is rejected the same way.
CREATE TABLE tab (a UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS auto_statistics_types = '';
ALTER TABLE tab MATERIALIZE STATISTICS nonexistent; -- { serverError ILLEGAL_STATISTICS }
DROP TABLE tab;

-- A physical column with statistics must keep building them.
CREATE TABLE tab (a UInt64, b UInt64 MATERIALIZED a * 2 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO tab (a) VALUES (1);
SELECT a, b FROM tab;
SELECT column, has(statistics, 'TDigest') FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY column;
DROP TABLE tab;
