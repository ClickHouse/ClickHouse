-- Tags: no-fasttest, use-rocksdb, no-parallel-replicas
-- A filter on a column named like another column's qualified name (`__table1.k` next to `k`) must use that column in
-- primary key, PREWHERE, row policy and storage key analysis.

DROP TABLE IF EXISTS t_dotted;
CREATE TABLE t_dotted (k UInt64, `__table1.k` UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_dotted SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_dotted WHERE `__table1.k` IN (1, 2);
SELECT count(), sum(k) FROM t_dotted PREWHERE `__table1.k` IN (1, 2);

DROP ROW POLICY IF EXISTS p_dotted ON t_dotted;
CREATE ROW POLICY p_dotted ON t_dotted USING `__table1.k` IN (1, 2) TO ALL;
SELECT count(), sum(k) FROM t_dotted;
DROP ROW POLICY p_dotted ON t_dotted;

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (k UInt64, `__table1` Tuple(k UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_tuple SELECT number, tuple(2000 - number) FROM numbers(2000);
SELECT count(), sum(k) FROM t_tuple WHERE `__table1`.k IN (1, 2);

DROP TABLE IF EXISTS t_rocksdb;
CREATE TABLE t_rocksdb (k UInt64, `__table1.k` UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO t_rocksdb SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_rocksdb WHERE `__table1.k` IN (1, 2);

DROP TABLE t_dotted;
DROP TABLE t_tuple;
DROP TABLE t_rocksdb;
