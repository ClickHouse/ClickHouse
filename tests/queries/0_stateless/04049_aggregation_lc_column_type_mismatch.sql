-- Tags: zookeeper, no-random-settings, no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100175

SET allow_suspicious_low_cardinality_types = 1;

-- Test 1: Int8 partition key → LowCardinality(Int8)
DROP TABLE IF EXISTS t_lc_agg SYNC;

CREATE TABLE t_lc_agg (key Int8, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lc_agg', 'r1')
PARTITION BY key ORDER BY key;

INSERT INTO t_lc_agg SELECT number % 5, number FROM numbers(10);

ALTER TABLE t_lc_agg MODIFY COLUMN key LowCardinality(Int8) SETTINGS alter_sync = 2;

SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

-- Test 2: reverse direction, LowCardinality(Int8) → Int8
ALTER TABLE t_lc_agg MODIFY COLUMN key Int8 SETTINGS alter_sync = 2;

SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

DROP TABLE t_lc_agg SYNC;

-- Test 3: multi-column partition key with Enum8 origin (original fuzzer scenario)
DROP TABLE IF EXISTS t_lc_agg_enum SYNC;

CREATE TABLE t_lc_agg_enum (product Enum8('A' = 1, 'B' = 2), ts DateTime, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lc_agg_enum', 'r1')
PARTITION BY (product, toYYYYMM(ts)) ORDER BY (product, ts);

INSERT INTO t_lc_agg_enum VALUES ('A', '2024-01-15 10:00:00', 1), ('B', '2024-02-20 12:00:00', 2);

ALTER TABLE t_lc_agg_enum MODIFY COLUMN product Int8 SETTINGS alter_sync = 2;
ALTER TABLE t_lc_agg_enum MODIFY COLUMN product LowCardinality(Int8) SETTINGS alter_sync = 2;

SELECT product FROM t_lc_agg_enum GROUP BY product ORDER BY product ASC;

DROP TABLE t_lc_agg_enum SYNC;
