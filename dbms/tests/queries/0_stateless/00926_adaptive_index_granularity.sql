DROP TABLE IF EXISTS test.adapted_index_granularity_table;

CREATE TABLE test.adapted_index_granularity_table (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 40;

INSERT INTO test.adapted_index_granularity_table (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.adapted_index_granularity_table;

OPTIMIZE TABLE test.adapted_index_granularity_table FINAL;


INSERT INTO test.adapted_index_granularity_table (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 7, 7000, 8000);

SELECT COUNT(*) FROM test.adapted_index_granularity_table;

DROP TABLE IF EXISTS test.adapted_index_granularity_table;
