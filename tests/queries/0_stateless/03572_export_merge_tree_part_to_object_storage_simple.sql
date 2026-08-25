-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS 03572_mt_table, 03572_invalid_schema_table, 03572_ephemeral_mt_table, 03572_matching_ephemeral_s3_table, 03572_partition_type_mismatch_mt, 03572_partition_type_mismatch_s3, 03572_lossy_mt, 03572_lossy_s3, 03572_lossless_mt, 03572_lossless_s3, 03572_coarser_source_mt, 03572_finer_dest_s3;

SET allow_experimental_export_merge_tree_part=1;

CREATE TABLE 03572_mt_table (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

INSERT INTO 03572_mt_table VALUES (1, 2020);

-- Create a table partitioned by a column that is not part of the source partition key. The unified
-- plain-storage partition gate rejects it because the destination partition column is not covered by
-- the source partition key (schema compat follows INSERT SELECT positional semantics, so the column
-- shape matches and the partition-compatibility check is what fires).
CREATE TABLE 03572_invalid_schema_table (id UInt64, x UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table', format='Parquet', partition_strategy='hive') PARTITION BY x;

ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_invalid_schema_table
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError BAD_ARGUMENTS}

DROP TABLE 03572_invalid_schema_table;

-- The only partition strategy that supports exports is hive. Wildcard should throw
CREATE TABLE 03572_invalid_schema_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table/{_partition_id}', format='Parquet', partition_strategy='wildcard') PARTITION BY (id, year);

ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_invalid_schema_table; -- {serverError NOT_IMPLEMENTED}

-- Not a table function, should throw
ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE FUNCTION extractKeyValuePairs('name:ronaldo'); -- {serverError UNKNOWN_FUNCTION}

-- It is a table function, but the engine does not support exports/imports, should throw
ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE FUNCTION url('a.parquet'); -- {serverError NOT_IMPLEMENTED}

-- Source-side ephemeral columns are not readable, so the destination must not declare a matching
-- ordinary column or the column count will not align under positional matching.
CREATE TABLE 03572_ephemeral_mt_table (id UInt64, year UInt16, name String EPHEMERAL) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

CREATE TABLE 03572_matching_ephemeral_s3_table (id UInt64, year UInt16, name String) ENGINE = S3(s3_conn, filename='03572_matching_ephemeral_s3_table', format='Parquet', partition_strategy='hive') PARTITION BY year;

INSERT INTO 03572_ephemeral_mt_table (id, year, name) VALUES (1, 2020, 'alice');

ALTER TABLE 03572_ephemeral_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_matching_ephemeral_s3_table; -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH}

-- Partition columns follow the same lossy-cast gate as any other column (no special
-- exact-type guard). String -> UInt16 is a lossy cast, so with the default
-- export_merge_tree_part_allow_lossy_cast = 0 it is rejected synchronously.
CREATE TABLE 03572_partition_type_mismatch_mt (id UInt64, year String) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_partition_type_mismatch_s3 (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='03572_partition_type_mismatch_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_partition_type_mismatch_mt EXPORT PART '2020_1_1_0' TO TABLE 03572_partition_type_mismatch_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError INCOMPATIBLE_COLUMNS}

CREATE TABLE 03572_lossy_mt (id Int64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_lossy_s3 (id Int32, year UInt16) ENGINE = S3(s3_conn, filename='03572_lossy_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_lossy_mt EXPORT PART '2020_1_1_0' TO TABLE 03572_lossy_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError INCOMPATIBLE_COLUMNS}

-- With the acknowledgment setting enabled, the lossy cast passes validation and reaches the
-- part lookup, which fails because the part does not exist.
ALTER TABLE 03572_lossy_mt EXPORT PART '2020_1_1_0' TO TABLE 03572_lossy_s3
SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_allow_lossy_cast = 1; -- {serverError NO_SUCH_DATA_PART}

-- A lossless widening cast (Int32 -> Int64) passes validation without the setting and reaches
-- the part lookup, which fails because the part does not exist.
CREATE TABLE 03572_lossless_mt (id Int32, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();
CREATE TABLE 03572_lossless_s3 (id Int64, year UInt16) ENGINE = S3(s3_conn, filename='03572_lossless_s3', format='Parquet', partition_strategy='hive') PARTITION BY year;

ALTER TABLE 03572_lossless_mt EXPORT PART '2020_1_1_0' TO TABLE 03572_lossless_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError NO_SUCH_DATA_PART}

-- Unified plain-storage partition gate: the destination partitioning must be single-valued within
-- each exported source part. The source is partitioned monthly (toYYYYMM(dt)) while the destination
-- is partitioned by the raw date, so a single source part holding two different days would map to two
-- destination partitions. The gate rejects it (the part exists, so the data-dependent check runs).
CREATE TABLE 03572_coarser_source_mt (id UInt64, dt Date) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY tuple();
CREATE TABLE 03572_finer_dest_s3 (id UInt64, dt Date) ENGINE = S3(s3_conn, filename='03572_finer_dest_s3', format='Parquet', partition_strategy='hive') PARTITION BY dt;

INSERT INTO 03572_coarser_source_mt VALUES (1, '2024-03-05'), (2, '2024-03-20');

ALTER TABLE 03572_coarser_source_mt EXPORT PART '202403_1_1_0' TO TABLE 03572_finer_dest_s3
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS 03572_mt_table, 03572_invalid_schema_table, 03572_ephemeral_mt_table, 03572_matching_ephemeral_s3_table, 03572_partition_type_mismatch_mt, 03572_partition_type_mismatch_s3, 03572_lossy_mt, 03572_lossy_s3, 03572_lossless_mt, 03572_lossless_s3, 03572_coarser_source_mt, 03572_finer_dest_s3;
