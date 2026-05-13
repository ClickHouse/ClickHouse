-- Tags: distributed
--
-- Sanity check that the old-analyzer rewrite for `count_distinct_optimization`
-- (in `InterpreterSelectQuery` via `RewriteCountDistinctFunctionVisitor`)
-- preserves correctness across the cases covered by its gates:
--   * remote tables (`Distributed` or `remote(...)`) where the rewrite is
--     skipped to avoid interfering with distributed aggregation;
--   * fixed-size numeric columns, including `Nullable(UInt64)` and
--     `LowCardinality(Nullable(UInt32))`, where the rewrite is skipped because
--     `uniqExact` beats the `GROUP BY` rewrite for these types.
--
-- The new-analyzer counterpart for these gates is covered by
-- `04212_count_distinct_optimization_remote.sql` and
-- `04218_count_distinct_optimization_numeric.sql`.

SET enable_analyzer = 0;
SET count_distinct_optimization = 1;
SET prefer_localhost_replica = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS data_04235;
DROP TABLE IF EXISTS dist_04235;
DROP TABLE IF EXISTS num_04235;
DROP TABLE IF EXISTS nullable_num_04235;
DROP TABLE IF EXISTS lc_nullable_num_04235;

CREATE TABLE data_04235             (s String)                           ENGINE = MergeTree ORDER BY s;
INSERT INTO data_04235 VALUES ('a'), ('a'), ('b'), ('b'), ('c');
CREATE TABLE dist_04235 AS data_04235 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_04235, cityHash64(s));

CREATE TABLE num_04235              (x UInt64)                           ENGINE = MergeTree ORDER BY tuple();
INSERT INTO num_04235 VALUES (1), (1), (2);
CREATE TABLE nullable_num_04235     (x Nullable(UInt64))                 ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nullable_num_04235 VALUES (1), (1), (2);
CREATE TABLE lc_nullable_num_04235  (x LowCardinality(Nullable(UInt32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO lc_nullable_num_04235 VALUES (1), (1), (2);

SELECT 'string',              count(DISTINCT s) FROM data_04235;
SELECT 'distributed',         count(DISTINCT s) FROM dist_04235;
SELECT 'remote()',            count(DISTINCT s) FROM remote('127.0.0.{1,2}', currentDatabase(), data_04235);
SELECT 'numeric',             count(DISTINCT x) FROM num_04235;
SELECT 'nullable numeric',    count(DISTINCT x) FROM nullable_num_04235;
SELECT 'LC nullable numeric', count(DISTINCT x) FROM lc_nullable_num_04235;

DROP TABLE dist_04235;
DROP TABLE data_04235;
DROP TABLE num_04235;
DROP TABLE nullable_num_04235;
DROP TABLE lc_nullable_num_04235;
