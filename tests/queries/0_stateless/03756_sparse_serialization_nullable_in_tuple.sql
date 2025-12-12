-- https://github.com/ClickHouse/ClickHouse/issues/91380
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (s Nullable(String), t Tuple(a Nullable(String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO t0 SELECT if((number % 13) = 0, toString(number), NULL), (if((number % 11) = 0, number, NULL),) FROM numbers(1000);
SELECT * FROM t0 FULL JOIN t0 ty ON ty.s = t.a WHERE t.a.size = 1;
