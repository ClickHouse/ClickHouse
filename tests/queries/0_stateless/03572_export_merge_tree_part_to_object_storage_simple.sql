-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS 03572_mt_table, 03572_invalid_schema_table, 03572_ephemeral_mt_table, 03572_matching_ephemeral_s3_table;

SET allow_experimental_export_merge_tree_part=1;

CREATE TABLE 03572_mt_table (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

INSERT INTO 03572_mt_table VALUES (1, 2020);

-- Create a table with a different partition key and export a partition to it. It should throw
CREATE TABLE 03572_invalid_schema_table (id UInt64, x UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table', format='Parquet', partition_strategy='hive') PARTITION BY x;

ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_invalid_schema_table
SETTINGS allow_experimental_export_merge_tree_part = 1; -- {serverError INCOMPATIBLE_COLUMNS}

DROP TABLE 03572_invalid_schema_table;

-- The only partition strategy that supports exports is hive. Wildcard should throw
CREATE TABLE 03572_invalid_schema_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='03572_invalid_schema_table/{_partition_id}', format='Parquet', partition_strategy='wildcard') PARTITION BY (id, year);

ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_invalid_schema_table; -- {serverError NOT_IMPLEMENTED}

-- Not a table function, should throw
ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE FUNCTION extractKeyValuePairs('name:ronaldo'); -- {serverError UNKNOWN_FUNCTION}

-- It is a table function, but the engine does not support exports/imports, should throw
ALTER TABLE 03572_mt_table EXPORT PART '2020_1_1_0' TO TABLE FUNCTION url('a.parquet'); -- {serverError NOT_IMPLEMENTED}

-- Test that destination table can not have a column that matches the source ephemeral
CREATE TABLE 03572_ephemeral_mt_table (id UInt64, year UInt16, name String EPHEMERAL) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

CREATE TABLE 03572_matching_ephemeral_s3_table (id UInt64, year UInt16, name String) ENGINE = S3(s3_conn, filename='03572_matching_ephemeral_s3_table', format='Parquet', partition_strategy='hive') PARTITION BY year;

INSERT INTO 03572_ephemeral_mt_table (id, year, name) VALUES (1, 2020, 'alice');

ALTER TABLE 03572_ephemeral_mt_table EXPORT PART '2020_1_1_0' TO TABLE 03572_matching_ephemeral_s3_table; -- {serverError INCOMPATIBLE_COLUMNS}

DROP TABLE IF EXISTS 03572_mt_table, 03572_invalid_schema_table, 03572_ephemeral_mt_table, 03572_matching_ephemeral_s3_table;
