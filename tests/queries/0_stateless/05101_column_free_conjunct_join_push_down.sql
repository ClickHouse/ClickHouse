-- Tags: no-fasttest
-- EmbeddedRocksDB is not available in the fast test build.
-- https://github.com/ClickHouse/ClickHouse/issues/113969
-- A conjunct with no columns satisfies every side's column check, so the join filter push-down
-- classified it as pushable to both sides. Pushing it interposes a `Filter` above the read, which
-- hides a key-value right side from the direct-join detection: a forced `join_algorithm = 'direct'`
-- then had no algorithm left and the query failed, only because of a provably-true conjunct.

DROP TABLE IF EXISTS t_direct_left;
DROP TABLE IF EXISTS t_direct_right;
CREATE TABLE t_direct_left (k UInt16) ENGINE = TinyLog;
INSERT INTO t_direct_left SELECT number FROM numbers(10);
CREATE TABLE t_direct_right (key UInt32, value Array(UInt32), value2 String) ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO t_direct_right SELECT number, [number], concat('val2', toString(number)) FROM numbers(7);

SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k ORDER BY value2 SETTINGS join_algorithm = 'direct';
SELECT 'materialize(1)';
SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k WHERE materialize(1) ORDER BY value2 SETTINGS join_algorithm = 'direct';
SELECT 'toNullable(256)';
SELECT count() FROM (SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k WHERE toNullable(256) SETTINGS join_algorithm = 'direct');
SELECT 'CAST to LowCardinality(Nullable)';
SELECT count() FROM (SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k WHERE CAST(1 AS LowCardinality(Nullable(UInt8))) SETTINGS join_algorithm = 'direct', allow_suspicious_low_cardinality_types = 1);
SELECT 'the conjunct still filters';
SELECT count() FROM (SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k WHERE materialize(0) SETTINGS join_algorithm = 'direct');
SELECT 'a column predicate is still pushed down';
SELECT count() FROM (SELECT value2 FROM t_direct_left LEFT JOIN t_direct_right ON t_direct_right.key == t_direct_left.k WHERE t_direct_left.k < 3 SETTINGS join_algorithm = 'direct');
DROP TABLE t_direct_left;
DROP TABLE t_direct_right;

SELECT 'an ordinary join keeps the same results';
DROP TABLE IF EXISTS t_join_left;
DROP TABLE IF EXISTS t_join_right;
CREATE TABLE t_join_left (k UInt32, a UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_join_right (k UInt32, b UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_join_left SELECT number, number FROM numbers(10);
INSERT INTO t_join_right SELECT number, number * 10 FROM numbers(5);
SELECT count(), sum(a), sum(b) FROM t_join_left LEFT JOIN t_join_right USING (k) WHERE materialize(1);
SELECT count(), sum(a), sum(b) FROM t_join_left LEFT JOIN t_join_right USING (k);
SELECT count(), sum(a), sum(b) FROM t_join_left LEFT JOIN t_join_right USING (k) WHERE materialize(1) SETTINGS query_plan_filter_push_down = 0;
SELECT count() FROM t_join_left LEFT JOIN t_join_right USING (k) WHERE materialize(toNullable(NULL));
SELECT count() FROM t_join_left LEFT JOIN t_join_right USING (k) WHERE materialize(toNullable(NULL)) SETTINGS query_plan_filter_push_down = 0;
SELECT count(), sum(a), sum(b) FROM t_join_left FULL JOIN t_join_right USING (k) WHERE materialize(1);
SELECT count(), sum(a), sum(b) FROM t_join_left FULL JOIN t_join_right USING (k) WHERE materialize(1) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_join_left;
DROP TABLE t_join_right;
