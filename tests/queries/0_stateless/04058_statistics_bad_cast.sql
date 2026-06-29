CREATE TABLE t (a UInt64) ENGINE = MergeTree() ORDER BY a settings auto_statistics_types = 'tdigest';
INSERT INTO t SELECT number FROM numbers(1000);
SELECT count(*) FROM t
WHERE (toDecimal64(a, 3) > toDecimal64(100, 3)) OR toDecimal64(a, 3) = '50.0';
DROP TABLE IF EXISTS t;

