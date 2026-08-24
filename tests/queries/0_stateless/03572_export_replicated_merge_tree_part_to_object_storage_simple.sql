-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS 03572_rmt_table, 03572_invalid_schema_table, 03572_rmt_partition_type_mismatch_mt, 03572_rmt_partition_type_mismatch_s3, 03572_rmt_lossy_mt, 03572_rmt_lossy_s3, 03572_rmt_lossless_mt, 03572_rmt_lossless_s3;

CREATE TABLE 03572_rmt_table (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03572_rmt/03572_rmt_table', 'replica1') PARTITION BY year ORDER BY tuple();

INSERT INTO 03572_rmt_table VALUES (1, 2020);

-- Create a table with a different partition key and export a partition to it. It should throw
-- on the partition-key AST mismatch (schema compat now follows INSERT SELECT positional semantics,
-- so the column shape matches and the partition-key check is what fires).
CREATE TABLE 03572_invalid_schema_table (id UInt64, x UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table', format='Parquet', partition_strategy='hive') PARTITION BY x;

ALTER TABLE 03572_rmt_table EXPORT PART '2020_0_0_0' TO TABLE 03572_invalid_schema_table
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError BAD_ARGUMENTS}

DROP TABLE 03572_invalid_schema_table;

-- The only partition strategy that supports exports is hive. Wildcard should throw
CREATE TABLE 03572_invalid_schema_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table/{_partition_id}', format='Parquet', partition_strategy='wildcard') PARTITION BY (id, year);

ALTER TABLE 03572_rmt_table EXPORT PART '2020_0_0_0' TO TABLE 03572_invalid_schema_table SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError NOT_IMPLEMENTED}

-- Partition columns follow the same lossy-cast gate as any other column (no special
-- exact-type guard). String -> UInt16 is a lossy cast, so with the default
-- export_merge_tree_part_allow_lossy_cast = 0 it is rejected synchronously.
CREATE TABLE 03572_rmt_partition_type_mismatch_mt (id UInt64, year String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03572_rmt_pcol_type/03572_rmt_partition_type_mismatch_mt', 'replica1') PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_rmt_partition_type_mismatch_s3 (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='03572_rmt_partition_type_mismatch_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_rmt_partition_type_mismatch_mt EXPORT PART '2020_0_0_0' TO TABLE 03572_rmt_partition_type_mismatch_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError INCOMPATIBLE_COLUMNS}

-- A lossy cast on a non-partition column (Int64 -> Int32) is rejected synchronously by default.
CREATE TABLE 03572_rmt_lossy_mt (id Int64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03572_rmt_lossy/03572_rmt_lossy_mt', 'replica1') PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_rmt_lossy_s3 (id Int32, year UInt16) ENGINE = S3(s3_conn, filename='03572_rmt_lossy_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_rmt_lossy_mt EXPORT PART '2020_0_0_0' TO TABLE 03572_rmt_lossy_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError INCOMPATIBLE_COLUMNS}

-- With the acknowledgment setting enabled, the lossy cast passes validation and reaches the
-- part lookup, which fails because the part does not exist.
ALTER TABLE 03572_rmt_lossy_mt EXPORT PART '2020_0_0_0' TO TABLE 03572_rmt_lossy_s3
SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_allow_lossy_cast = 1; -- {serverError NO_SUCH_DATA_PART}

-- A lossless widening cast (Int32 -> Int64) passes validation without the setting and reaches
-- the part lookup, which fails because the part does not exist.
CREATE TABLE 03572_rmt_lossless_mt (id Int32, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03572_rmt_lossless/03572_rmt_lossless_mt', 'replica1') PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_rmt_lossless_s3 (id Int64, year UInt16) ENGINE = S3(s3_conn, filename='03572_rmt_lossless_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_rmt_lossless_mt EXPORT PART '2020_0_0_0' TO TABLE 03572_rmt_lossless_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError NO_SUCH_DATA_PART}

DROP TABLE IF EXISTS 03572_rmt_table, 03572_invalid_schema_table, 03572_rmt_partition_type_mismatch_mt, 03572_rmt_partition_type_mismatch_s3, 03572_rmt_lossy_mt, 03572_rmt_lossy_s3, 03572_rmt_lossless_mt, 03572_rmt_lossless_s3;
