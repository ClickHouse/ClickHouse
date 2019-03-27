----- Group of very similar simple tests ------
DROP TABLE IF EXISTS test.zero_rows_per_granule;

CREATE TABLE test.zero_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64,
  Sign Int8,
  Version UInt8
) ENGINE VersionedCollapsingMergeTree(Sign, Version) PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=20,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.zero_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, 1, 1), ('2018-05-16', 2, 3000, 4000, 1, 1), ('2018-05-17', 3, 5000, 6000, 1, 1), ('2018-05-18', 4, 7000, 8000, 1, 1);

SELECT COUNT(*) FROM test.zero_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

INSERT INTO test.zero_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 5, 1000, 2000, 1, 1), ('2018-05-16', 6, 3000, 4000, 1, 1), ('2018-05-17', 7, 5000, 6000, 1, 1), ('2018-05-19', 8, 7000, 8000, 1, 1);

OPTIMIZE TABLE test.zero_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.zero_rows_per_granule FINAL;

SELECT sum(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.zero_rows_per_granule;

SELECT '-----';
DROP TABLE IF EXISTS test.four_rows_per_granule;

CREATE TABLE test.four_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64,
  Sign Int8,
  Version UInt8
) ENGINE VersionedCollapsingMergeTree(Sign, Version) PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=120,
  enable_vertical_merge_algorithm=1,
  vertical_merge_algorithm_min_rows_to_activate=0,
  vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, 1, 1), ('2018-05-16', 2, 3000, 4000, 1, 1), ('2018-05-17', 3, 5000, 6000, 1, 1), ('2018-05-18', 4, 7000, 8000, 1, 1);

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, -1, 1), ('2018-05-16', 2, 3000, 4000, -1, 1), ('2018-05-17', 3, 5000, 6000, -1, 1), ('2018-05-18', 4, 7000, 8000, -1, 1);

OPTIMIZE TABLE test.four_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, 1, 1), ('2018-05-16', 2, 3000, 4000, 1, 1), ('2018-05-17', 3, 5000, 6000, 1, 1), ('2018-05-18', 4, 7000, 8000, 1, 1);

INSERT INTO test.four_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 5, 1000, 2000, 1, 1), ('2018-05-16', 6, 3000, 4000, 1, 1), ('2018-05-17', 7, 5000, 6000, 1, 1), ('2018-05-18', 8, 7000, 8000, 1, 1);

INSERT INTO test.four_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 5, 1000, 2000, -1, 1), ('2018-05-17', 7, 5000, 6000, -1, 1);

OPTIMIZE TABLE test.four_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.four_rows_per_granule;

SELECT '-----';

DROP TABLE IF EXISTS test.six_rows_per_granule;

CREATE TABLE test.six_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64,
  Sign Int8,
  Version UInt8
) ENGINE VersionedCollapsingMergeTree(Sign, Version) PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=170,
  enable_vertical_merge_algorithm=1,
  vertical_merge_algorithm_min_rows_to_activate=0,
  vertical_merge_algorithm_min_columns_to_activate=0;


INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, 1, 1), ('2018-05-16', 1, 1000, 2000, -1, 2);

INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 2, 1000, 2000, 1, 1), ('2018-05-16', 2, 1000, 2000, -1, 2);

INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 3, 1000, 2000, 1, 1), ('2018-05-16', 3, 1000, 2000, -1, 2);

OPTIMIZE TABLE test.six_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.six_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'six_rows_per_granule' and database='test' and active=1;

INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 1, 1000, 2000, -1, 1), ('2018-05-16', 1, 1000, 2000, 1, 2);

INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 2, 1000, 2000, -1, 1), ('2018-05-16', 2, 1000, 2000, 1, 2);

INSERT INTO test.six_rows_per_granule (p, k, v1, v2, Sign, Version) VALUES ('2018-05-15', 3, 1000, 2000, -1, 1), ('2018-05-16', 3, 1000, 2000, 1, 2);

OPTIMIZE TABLE test.six_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.six_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'six_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.six_rows_per_granule;
