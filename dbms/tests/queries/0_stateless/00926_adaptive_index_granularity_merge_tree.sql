----- Group of very similar simple tests ------
select '----HORIZONTAL MERGE TESTS----';
DROP TABLE IF EXISTS test.zero_rows_per_granule;

CREATE TABLE test.zero_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 20;

INSERT INTO test.zero_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.zero_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

INSERT INTO test.zero_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.zero_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.zero_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.zero_rows_per_granule;

SELECT '-----';

DROP TABLE IF EXISTS test.two_rows_per_granule;

CREATE TABLE test.two_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 40;

INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.two_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'two_rows_per_granule' and database='test' and active=1;

INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.two_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.two_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'two_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.two_rows_per_granule;

SELECT '-----';

DROP TABLE IF EXISTS test.four_rows_per_granule;

CREATE TABLE test.four_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;
DETACH TABLE test.four_rows_per_granule;
ATTACH TABLE test.four_rows_per_granule;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.four_rows_per_granule FINAL;

DETACH TABLE test.four_rows_per_granule;

ATTACH TABLE test.four_rows_per_granule;

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.four_rows_per_granule;

-----  More interesting tests ------
SELECT '-----';

DROP TABLE IF EXISTS test.huge_granularity_small_blocks;

CREATE TABLE test.huge_granularity_small_blocks (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 1000000;

INSERT INTO test.huge_granularity_small_blocks (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.huge_granularity_small_blocks;

SELECT distinct(marks) from system.parts WHERE table = 'huge_granularity_small_blocks' and database='test' and active=1;

INSERT INTO test.huge_granularity_small_blocks (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 7, 7000, 8000);

DETACH TABLE test.huge_granularity_small_blocks;

ATTACH TABLE test.huge_granularity_small_blocks;

OPTIMIZE TABLE test.huge_granularity_small_blocks FINAL;

SELECT COUNT(*) FROM test.huge_granularity_small_blocks;

SELECT distinct(marks) from system.parts WHERE table = 'huge_granularity_small_blocks' and database='test' and active=1;

DROP TABLE IF EXISTS test.huge_granularity_small_blocks;

SELECT '-----';

DROP TABLE IF EXISTS test.adaptive_granularity_alter;

CREATE TABLE test.adaptive_granularity_alter (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

OPTIMIZE TABLE test.adaptive_granularity_alter FINAL;

ALTER TABLE test.adaptive_granularity_alter MODIFY COLUMN v1 Int16;

DETACH TABLE test.adaptive_granularity_alter;

ATTACH TABLE test.adaptive_granularity_alter;

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 42, 42, 42);

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

ALTER TABLE test.adaptive_granularity_alter MODIFY COLUMN v2 String;

DETACH TABLE test.adaptive_granularity_alter;

ATTACH TABLE test.adaptive_granularity_alter;

INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 100, 1000, 'aaaa'), ('2018-05-16', 101, 3000, 'bbbb'), ('2018-05-17', 102, 5000, 'cccc'), ('2018-05-19', 103, 7000, 'dddd');

OPTIMIZE TABLE test.adaptive_granularity_alter FINAL;

SELECT k, v2 FROM test.adaptive_granularity_alter WHERE k >= 100 OR k = 42;

SELECT sum(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

DROP TABLE IF EXISTS test.adaptive_granularity_alter;

-----------------------------------------------
-------------VERTICAL MERGE TESTS--------------
-----------------------------------------------
select '----VERTICAL MERGE TESTS----';
DROP TABLE IF EXISTS test.zero_rows_per_granule;

CREATE TABLE test.zero_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=20,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;


INSERT INTO test.zero_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.zero_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

INSERT INTO test.zero_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.zero_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.zero_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.zero_rows_per_granule;

SELECT '-----';

DROP TABLE IF EXISTS test.two_rows_per_granule;

CREATE TABLE test.two_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=40,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.two_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'two_rows_per_granule' and database='test' and active=1;

INSERT INTO test.two_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.two_rows_per_granule FINAL;

SELECT COUNT(*) FROM test.two_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'two_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.two_rows_per_granule;

SELECT '-----';

DROP TABLE IF EXISTS test.four_rows_per_granule;

CREATE TABLE test.four_rows_per_granule (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes = 110,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;
DETACH TABLE test.four_rows_per_granule;
ATTACH TABLE test.four_rows_per_granule;

INSERT INTO test.four_rows_per_granule (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

OPTIMIZE TABLE test.four_rows_per_granule FINAL;

DETACH TABLE test.four_rows_per_granule;

ATTACH TABLE test.four_rows_per_granule;

SELECT COUNT(*) FROM test.four_rows_per_granule;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule' and database='test' and active=1;

DROP TABLE IF EXISTS test.four_rows_per_granule;

-----  More interesting tests ------
SELECT '-----';

DROP TABLE IF EXISTS test.huge_granularity_small_blocks;

CREATE TABLE test.huge_granularity_small_blocks (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=1000000,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.huge_granularity_small_blocks (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.huge_granularity_small_blocks;

SELECT distinct(marks) from system.parts WHERE table = 'huge_granularity_small_blocks' and database='test' and active=1;

INSERT INTO test.huge_granularity_small_blocks (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 7, 7000, 8000);

DETACH TABLE test.huge_granularity_small_blocks;

ATTACH TABLE test.huge_granularity_small_blocks;

OPTIMIZE TABLE test.huge_granularity_small_blocks FINAL;

SELECT COUNT(*) FROM test.huge_granularity_small_blocks;

SELECT distinct(marks) from system.parts WHERE table = 'huge_granularity_small_blocks' and database='test' and active=1;

DROP TABLE IF EXISTS test.huge_granularity_small_blocks;

SELECT '-----';

DROP TABLE IF EXISTS test.adaptive_granularity_alter;

CREATE TABLE test.adaptive_granularity_alter (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k
  SETTINGS index_granularity_bytes=110,
           enable_vertical_merge_algorithm=1,
           vertical_merge_algorithm_min_rows_to_activate=0,
           vertical_merge_algorithm_min_columns_to_activate=0;


INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

OPTIMIZE TABLE test.adaptive_granularity_alter FINAL;

ALTER TABLE test.adaptive_granularity_alter MODIFY COLUMN v1 Int16;

DETACH TABLE test.adaptive_granularity_alter;

ATTACH TABLE test.adaptive_granularity_alter;

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 42, 42, 42);

SELECT COUNT(*) FROM test.adaptive_granularity_alter;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

ALTER TABLE test.adaptive_granularity_alter MODIFY COLUMN v2 String;

DETACH TABLE test.adaptive_granularity_alter;

ATTACH TABLE test.adaptive_granularity_alter;

INSERT INTO test.adaptive_granularity_alter (p, k, v1, v2) VALUES ('2018-05-15', 100, 1000, 'aaaa'), ('2018-05-16', 101, 3000, 'bbbb'), ('2018-05-17', 102, 5000, 'cccc'), ('2018-05-19', 103, 7000, 'dddd');

OPTIMIZE TABLE test.adaptive_granularity_alter FINAL;

SELECT k, v2 FROM test.adaptive_granularity_alter WHERE k >= 100 OR k = 42;

SELECT sum(marks) from system.parts WHERE table = 'adaptive_granularity_alter' and database='test' and active=1;

DROP TABLE IF EXISTS test.adaptive_granularity_alter;
