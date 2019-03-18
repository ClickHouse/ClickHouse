DROP TABLE IF EXISTS test.two_rows_per_granule;

CREATE TABLE test.two_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 40;

INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.two_rows_per_granule;

OPTIMIZE TABLE test.two_rows_per_granule FINAL;


INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 7, 7000, 8000);

SELECT COUNT(*) FROM test.two_rows_per_granule;

DROP TABLE IF EXISTS test.two_rows_per_granule;

DROP TABLE IF EXISTS test.two_rows_per_granule;

CREATE TABLE test.four_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 40;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.four_rows_per_granule;

OPTIMIZE TABLE test.four_rows_per_granule FINAL;


INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 7, 7000, 8000);

SELECT COUNT(*) FROM test.four_rows_per_granule;

DROP TABLE IF EXISTS test.four_rows_per_granule;
