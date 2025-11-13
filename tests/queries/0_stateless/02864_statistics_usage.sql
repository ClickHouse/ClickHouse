-- Test that the optimizer picks up column statistics
-- (The concrete statistics type, column data type and predicate type don't matter)

-- Checks by the predicate evaluation order in EXPLAIN. This is quite fragile, a better approach would be helpful (maybe 'send_logs_level'?)

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = '';

INSERT INTO tab select number, -number FROM system.numbers LIMIT 10000;
SELECT 'After insert';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then b (statistics used)

ALTER TABLE tab DROP STATISTICS a, b;
SELECT 'After drop statistic';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks b first, then a (statistics not used)

SELECT name, column, statistics from system.parts_columns where (database = currentDatabase()) AND (table = 'tab');
ALTER TABLE tab ADD STATISTICS a, b TYPE tdigest;
ALTER TABLE tab MATERIALIZE STATISTICS ALL;
SELECT name, column, statistics from system.parts_columns where (database = currentDatabase()) AND (table = 'tab');
INSERT INTO tab select number, -number FROM system.numbers LIMIT 10000;
SELECT 'After add and materialize statistic';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then b (statistics used)

OPTIMIZE TABLE tab FINAL;
SELECT 'After merge';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then b (statistics used)

ALTER TABLE tab RENAME COLUMN b TO c;
SELECT 'After rename';
SELECT replaceRegexpAll(explain, '__table1\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE c < 10 and a < 10) WHERE explain LIKE '%Prewhere%'; -- checks a first, then c (statistics used)

DROP TABLE IF EXISTS tab;
