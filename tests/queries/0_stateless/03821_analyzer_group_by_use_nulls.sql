SET enable_analyzer = 1;

CREATE TABLE t (x LowCardinality(String)) ENGINE=Memory;
INSERT INTO t VALUES ('a');
SELECT x IS NULL, x FROM t GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls=1;

-- https://github.com/ClickHouse/ClickHouse/issues/95299
-- These previously crashed with `Block structure mismatch ... LowCardinality(UInt16) vs LowCardinality(Nullable(UInt16))`
-- in `AggregatingStep::transformPipeline` (GROUPING SETS path, missing-keys DAG)
-- and `CubeStep::transformPipeline` (`addGroupingSetForTotals`).
SELECT '---';
SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t2 (a LowCardinality(UInt16), b LowCardinality(UInt16)) ENGINE = Memory;
INSERT INTO t2 VALUES (1024, 2048);
SELECT a, b FROM t2 GROUP BY GROUPING SETS ((a, b), (a)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT '---';
SELECT a, b FROM t2 GROUP BY a, b WITH CUBE ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT '---';
SELECT a, b FROM t2 GROUP BY a, b WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1;
