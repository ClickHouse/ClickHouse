----- Group of very similar simple tests ------
select '----HORIZONTAL MERGE TESTS----';
DROP TABLE IF EXISTS test.zero_rows_per_granule1;
DROP TABLE IF EXISTS test.zero_rows_per_granule2;

CREATE TABLE test.zero_rows_per_granule1 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/zero_rows_in_granule', '1') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 20;

CREATE TABLE test.zero_rows_per_granule2 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/zero_rows_in_granule', '2') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 20;

INSERT INTO test.zero_rows_per_granule1 (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SYSTEM SYNC REPLICA test.zero_rows_per_granule2;

SELECT COUNT(*) FROM test.zero_rows_per_granule1;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule1' and database='test' and active=1;

SELECT COUNT(*) FROM test.zero_rows_per_granule2;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule2' and database='test' and active=1;

INSERT INTO test.zero_rows_per_granule2 (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule2' and database='test' and active=1;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule1' and database='test' and active=1;

SELECT sleep(0.7) Format Null;

OPTIMIZE TABLE test.zero_rows_per_granule2 FINAL;

SYSTEM SYNC REPLICA test.zero_rows_per_granule1;

SELECT COUNT(*) FROM test.zero_rows_per_granule2;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule2' and database='test' and active=1;

SELECT COUNT(*) FROM test.zero_rows_per_granule1;

SELECT distinct(marks) from system.parts WHERE table = 'zero_rows_per_granule1' and database='test' and active=1;

DROP TABLE IF EXISTS test.zero_rows_per_granule1;
DROP TABLE IF EXISTS test.zero_rows_per_granule2;

SELECT '-----';

DROP TABLE IF EXISTS test.four_rows_per_granule1;
DROP TABLE IF EXISTS test.four_rows_per_granule2;

CREATE TABLE test.four_rows_per_granule1 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/four_rows_in_granule', '1') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

CREATE TABLE test.four_rows_per_granule2 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/four_rows_in_granule', '2') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

INSERT INTO test.four_rows_per_granule1 (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.four_rows_per_granule1;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule1' and database='test' and active=1;

SYSTEM SYNC REPLICA test.four_rows_per_granule2;

SELECT COUNT(*) FROM test.four_rows_per_granule2;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule2' and database='test' and active=1;

DETACH TABLE test.four_rows_per_granule2;
ATTACH TABLE test.four_rows_per_granule2;

INSERT INTO test.four_rows_per_granule2 (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-19', 8, 7000, 8000);

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule2' and database='test' and active=1;

SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule1' and database='test' and active=1;

SELECT sleep(0.7) Format Null;

OPTIMIZE TABLE test.four_rows_per_granule2 FINAL;

DETACH TABLE test.four_rows_per_granule2;

ATTACH TABLE test.four_rows_per_granule2;

SELECT COUNT(*) FROM test.four_rows_per_granule2;

--SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule2' and database='test' and active=1;

SYSTEM SYNC REPLICA test.four_rows_per_granule1;

SELECT COUNT(*) FROM test.four_rows_per_granule1;

--SELECT distinct(marks) from system.parts WHERE table = 'four_rows_per_granule1' and database='test' and active=1;

DROP TABLE IF EXISTS test.four_rows_per_granule1;
DROP TABLE IF EXISTS test.four_rows_per_granule2;

SELECT '-----';

DROP TABLE IF EXISTS test.adaptive_granularity_alter1;
DROP TABLE IF EXISTS test.adaptive_granularity_alter2;

CREATE TABLE test.adaptive_granularity_alter1 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/adaptive_granularity_alter', '1') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

CREATE TABLE test.adaptive_granularity_alter2 (
  p Date,
  k UInt64,
  v1 UInt64,
  v2 Int64
) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/adaptive_granularity_alter', '2') PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS index_granularity_bytes = 110;

INSERT INTO test.adaptive_granularity_alter1 (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

SELECT COUNT(*) FROM test.adaptive_granularity_alter1;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter1' and database='test' and active=1;

SYSTEM SYNC REPLICA test.adaptive_granularity_alter2;

SELECT COUNT(*) FROM test.adaptive_granularity_alter2;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter2' and database='test' and active=1;

ALTER TABLE test.adaptive_granularity_alter2 MODIFY COLUMN v1 Int16;

DETACH TABLE test.adaptive_granularity_alter2;

ATTACH TABLE test.adaptive_granularity_alter2;

SELECT COUNT(*) FROM test.adaptive_granularity_alter2;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter2' and database='test' and active=1;

SYSTEM SYNC REPLICA test.adaptive_granularity_alter1;

SELECT COUNT(*) FROM test.adaptive_granularity_alter1;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter1' and database='test' and active=1;

INSERT INTO test.adaptive_granularity_alter1 (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 5, 3000, 4000), ('2018-05-17', 6, 5000, 6000), ('2018-05-19', 42, 42, 42);

SELECT COUNT(*) FROM test.adaptive_granularity_alter1;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter1' and database='test' and active=1;

SYSTEM SYNC REPLICA test.adaptive_granularity_alter2;

SELECT COUNT(*) FROM test.adaptive_granularity_alter2;

SELECT distinct(marks) from system.parts WHERE table = 'adaptive_granularity_alter2' and database='test' and active=1;

ALTER TABLE test.adaptive_granularity_alter1 MODIFY COLUMN v2 String;

DETACH TABLE test.adaptive_granularity_alter1;

ATTACH TABLE test.adaptive_granularity_alter1;

INSERT INTO test.adaptive_granularity_alter1 (p, k, v1, v2) VALUES ('2018-05-15', 100, 1000, 'aaaa'), ('2018-05-16', 101, 3000, 'bbbb'), ('2018-05-17', 102, 5000, 'cccc'), ('2018-05-19', 103, 7000, 'dddd');

SELECT sleep(0.7) Format Null;

OPTIMIZE TABLE test.adaptive_granularity_alter1 FINAL;

SELECT k, v2 FROM test.adaptive_granularity_alter1 WHERE k >= 100 OR k = 42;

SELECT sum(marks) from system.parts WHERE table = 'adaptive_granularity_alter1' and database='test' and active=1;

SYSTEM SYNC REPLICA test.adaptive_granularity_alter2;

SELECT k, v2 FROM test.adaptive_granularity_alter2 WHERE k >= 100 OR k = 42;

SELECT sum(marks) from system.parts WHERE table = 'adaptive_granularity_alter2' and database='test' and active=1;

DROP TABLE IF EXISTS test.adaptive_granularity_alter1;
DROP TABLE IF EXISTS test.adaptive_granularity_alter2;
