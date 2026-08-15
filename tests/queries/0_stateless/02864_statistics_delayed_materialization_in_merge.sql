-- Tests delayed materialization of statistics in merge instead of during insert (setting 'materialize_statistics_on_insert = 0').
-- (The concrete statistics type, column data type and predicate type don't matter)

-- Checks by the predicate evaluation order in EXPLAIN. This is quite fragile, a better approach would be helpful (maybe 'send_logs_level'?)

DROP TABLE IF EXISTS tab;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET enable_analyzer = 1;

SET materialize_statistics_on_insert = 0;

CREATE TABLE tab
(
    a Int64 STATISTICS(tdigest),
    b Int16 STATISTICS(tdigest),
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0; -- TODO: there is a bug in vertical merge with statistics.

INSERT INTO tab SELECT number, -number FROM system.numbers LIMIT 10000;
SELECT 'After insert';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks b first, then a (statistics not used)

OPTIMIZE TABLE tab FINAL;
SELECT 'After merge';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then b (statistics used)

TRUNCATE TABLE tab;
SET mutations_sync = 2;
INSERT INTO tab SELECT number, -number FROM system.numbers LIMIT 10000;
ALTER TABLE tab MATERIALIZE STATISTICS a, b;
SELECT 'After truncate, insert, and materialize';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then b (statistics used)

DROP TABLE tab;
