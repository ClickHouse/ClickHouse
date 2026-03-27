-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100175
-- After ALTER TABLE MODIFY COLUMN to LowCardinality, old data parts may still
-- have the pre-ALTER column type. The aggregation engine must handle the type
-- mismatch between the chosen LowCardinality hash method and the actual column.

SET mutations_sync = 0; -- do not wait for mutations

DROP TABLE IF EXISTS t_lc_agg;

CREATE TABLE t_lc_agg (key Int8, val UInt64)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_lc_agg SELECT number % 10, number FROM numbers(1000);

ALTER TABLE t_lc_agg MODIFY COLUMN key LowCardinality(Int8);

-- GROUP BY on a column that the schema says is LowCardinality but old parts
-- still store as plain Int8. This must not throw a logical error.
SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

-- Also test the reverse: alter back from LowCardinality to plain.
ALTER TABLE t_lc_agg MODIFY COLUMN key Int8;

SELECT key, sum(val) FROM t_lc_agg GROUP BY key ORDER BY key;

DROP TABLE t_lc_agg;
