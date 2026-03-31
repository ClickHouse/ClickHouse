-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100175
-- After ALTER TABLE MODIFY COLUMN to/from LowCardinality, old data parts may
-- still have the pre-ALTER column type. The aggregation engine must handle the
-- type mismatch between the chosen hash method and the actual column.

SET mutations_sync = 0; -- do not wait for mutations
SET allow_suspicious_low_cardinality_types = 1;

-- Single-key test: plain -> LowCardinality -> plain
DROP TABLE IF EXISTS t_lc_agg;

CREATE TABLE t_lc_agg (key Int8, val UInt64)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_lc_agg SELECT number % 10, number FROM numbers(1000);

ALTER TABLE t_lc_agg MODIFY COLUMN key LowCardinality(Int8);

SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

ALTER TABLE t_lc_agg MODIFY COLUMN key Int8;

SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

DROP TABLE t_lc_agg;

-- Multi-key test: mixed LowCardinality and plain keys with both alter directions.
-- Covers low_cardinality_keys128/256 variants where not all keys are LC.
DROP TABLE IF EXISTS t_lc_agg_multi;

CREATE TABLE t_lc_agg_multi (k1 Int8, k2 LowCardinality(String), val UInt64)
ENGINE = MergeTree ORDER BY (k1, k2);

INSERT INTO t_lc_agg_multi SELECT number % 5, toString(number % 3), number FROM numbers(1000);

-- Change plain key to LC and LC key to plain simultaneously.
ALTER TABLE t_lc_agg_multi MODIFY COLUMN k1 LowCardinality(Int8);
ALTER TABLE t_lc_agg_multi MODIFY COLUMN k2 String;

SELECT k1, k2, sum(val) FROM t_lc_agg_multi GROUP BY k1, k2 ORDER BY k1, k2;

-- Change them back.
ALTER TABLE t_lc_agg_multi MODIFY COLUMN k1 Int8;
ALTER TABLE t_lc_agg_multi MODIFY COLUMN k2 LowCardinality(String);

SELECT k1, k2, sum(val) FROM t_lc_agg_multi GROUP BY k1, k2 ORDER BY k1, k2;

DROP TABLE t_lc_agg_multi;
