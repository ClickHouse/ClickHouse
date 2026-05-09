-- Tests delayed materialization of statistics in merge instead of during insert (setting 'materialize_statistics_on_insert = 0').
-- (The concrete statistics type, column data type and predicate type don't matter)

-- Checks by the predicate evaluation order in EXPLAIN. This is quite fragile, a better approach would be helpful (maybe 'send_logs_level'?)

DROP TABLE IF EXISTS tab;

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;

SET materialize_statistics_on_insert = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False; statistics-based reordering of prewhere conditions suppressed → b,a stays instead of being reordered to a,b after statistics are built

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

-- Asymmetric merge with NullCount: part 1 has NullCount via explicit MATERIALIZE,
-- part 2 is inserted with materialize_statistics_on_insert = 0 (no stats file).
-- After OPTIMIZE FINAL, the merged part must still have NullCount because
-- MergeTask rebuilds missing stats from the data stream (see MergeTask::createMergedStream).
DROP TABLE IF EXISTS tab_nc;
CREATE TABLE tab_nc (
    id UInt64,
    value Nullable(Int64)
) ENGINE = MergeTree() ORDER BY id
SETTINGS auto_statistics_types = '';

SET materialize_statistics_on_insert = 0;
INSERT INTO tab_nc SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(100);
ALTER TABLE tab_nc ADD STATISTICS value TYPE nullcount;
ALTER TABLE tab_nc MATERIALIZE STATISTICS value;
INSERT INTO tab_nc SELECT number + 100, if(number % 3 = 0, NULL, number + 100) FROM numbers(100);

SELECT 'NullCount: part 1 has it, part 2 does not (asymmetric)';
SELECT name, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab_nc' AND active AND column = 'value'
ORDER BY name;

OPTIMIZE TABLE tab_nc FINAL;

SELECT 'NullCount: merged part has it (rebuilt during merge)';
SELECT name, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab_nc' AND active AND column = 'value'
ORDER BY name;

DROP TABLE tab_nc;
