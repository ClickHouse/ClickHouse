#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: MinIO and the Parquet write path are not available in the Fast test image.
#
# With the `hive` partition strategy the partition columns are, by default, kept out of the data
# file (`partition_columns_in_data_file = 0`), so the header the Parquet writer receives is not the
# table's declared column list. The definition-time validation of the `field_id` settings must run
# against the writer header: a map naming a partition column must be rejected up front (it would
# fail every `INSERT`), and a map covering only the written columns must be accepted (it used to be
# rejected as non-covering). The paths embed the database name so that concurrent runs do not race.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="04870_field_ids_hive_${CLICKHOUSE_DATABASE}"

# The partition column `p` is not in the writer header, so a map naming it is rejected at CREATE.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_field_ids_hive_reject (p Int32, x Int64)
    ENGINE = S3(s3_conn, filename = '${DATA_DIR}/reject', format = Parquet, partition_strategy = 'hive')
    PARTITION BY p
    SETTINGS output_format_parquet_column_field_ids = {'p': '1', 'x': '2'};
" 2>&1 | grep -o -m1 "output_format_parquet_column_field_ids references unknown column 'p'"

# A map covering exactly the written columns is valid for the same definition.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_field_ids_hive_written (p Int32, x Int64)
    ENGINE = S3(s3_conn, filename = '${DATA_DIR}/written', format = Parquet, partition_strategy = 'hive')
    PARTITION BY p
    SETTINGS output_format_parquet_column_field_ids = {'x': '1'};
    INSERT INTO t_field_ids_hive_written VALUES (1, 10), (2, 20);
    SELECT p, x FROM t_field_ids_hive_written ORDER BY p;
    DROP TABLE t_field_ids_hive_written;
"

# With `partition_columns_in_data_file = 1` the writer header is the full column list again:
# the full map is required and accepted, and a partial map is rejected as non-covering.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_field_ids_hive_in_file (p Int32, x Int64)
    ENGINE = S3(s3_conn, filename = '${DATA_DIR}/in_file', format = Parquet, partition_strategy = 'hive', partition_columns_in_data_file = 1)
    PARTITION BY p
    SETTINGS output_format_parquet_column_field_ids = {'p': '1', 'x': '2'};
    INSERT INTO t_field_ids_hive_in_file VALUES (3, 30);
    SELECT p, x FROM t_field_ids_hive_in_file;
    DROP TABLE t_field_ids_hive_in_file;
"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_field_ids_hive_partial (p Int32, x Int64)
    ENGINE = S3(s3_conn, filename = '${DATA_DIR}/partial', format = Parquet, partition_strategy = 'hive', partition_columns_in_data_file = 1)
    PARTITION BY p
    SETTINGS output_format_parquet_column_field_ids = {'x': '1'};
" 2>&1 | grep -o -m1 "does not cover every output column"

# Auto-assign works with a partitioned definition: it numbers whatever header the writer receives.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_field_ids_hive_auto (p Int32, x Int64)
    ENGINE = S3(s3_conn, filename = '${DATA_DIR}/auto', format = Parquet, partition_strategy = 'hive')
    PARTITION BY p
    SETTINGS output_format_parquet_auto_assign_field_ids = 1;
    INSERT INTO t_field_ids_hive_auto VALUES (4, 40);
    SELECT p, x FROM t_field_ids_hive_auto;
    DROP TABLE t_field_ids_hive_auto;
"
