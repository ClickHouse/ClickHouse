#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: the Parquet write path, pyarrow and MinIO are not available in the Fast test image.
#
# An ambient (session or profile) value of the Parquet `field_id` settings is never frozen onto a
# table definition of an engine that freezes its format settings (`File`, `URL`, the object-storage
# engines). Such a value was not written for that table: freezing it would leave the table with
# `field_id`s that were never checked against its columns, so every later `INSERT` would fail.
# The file paths embed the database name so that concurrent runs of this test do not race.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="04841_field_ids_ambient_${CLICKHOUSE_DATABASE}"

dump_field_ids () {
    python3 -c "
import pyarrow.parquet as pq
for field in pq.read_schema('$1'):
    print(field.name, (field.metadata or {}).get(b'PARQUET:field_id', b'-').decode(), sep='\t')
"
}

# An ambient map naming a column the table does not have would fail every `INSERT` if it were
# frozen onto the definition.
$CLICKHOUSE_CLIENT -q "
    SET output_format_parquet_column_field_ids = {'missing': '1'};
    CREATE TABLE t_field_ids_ambient_map (x Int64) ENGINE = File(Parquet, '${DATA_DIR}/map.parquet');
    INSERT INTO t_field_ids_ambient_map VALUES (1);
    SELECT * FROM t_field_ids_ambient_map;
    DROP TABLE t_field_ids_ambient_map;
"

# An ambient auto-assign does not annotate the table's output either.
$CLICKHOUSE_CLIENT -q "
    SET output_format_parquet_auto_assign_field_ids = 1;
    CREATE TABLE t_field_ids_ambient_auto (x Int64) ENGINE = File(Parquet, '${DATA_DIR}/auto.parquet');
    INSERT INTO t_field_ids_ambient_auto VALUES (2);
    SELECT * FROM t_field_ids_ambient_auto;
    DROP TABLE t_field_ids_ambient_auto;
"
dump_field_ids "${USER_FILES_PATH:?}/${DATA_DIR}/auto.parquet"

# The object-storage engines freeze their format settings the same way.
$CLICKHOUSE_CLIENT -q "
    SET output_format_parquet_column_field_ids = {'missing': '1'};
    CREATE TABLE t_field_ids_ambient_s3 (x Int64) ENGINE = S3('http://localhost:11111/test/${DATA_DIR}/s3.parquet', 'test', 'testtest', Parquet);
    INSERT INTO t_field_ids_ambient_s3 VALUES (3);
    SELECT * FROM t_field_ids_ambient_s3;
    DROP TABLE t_field_ids_ambient_s3;
"

# A definition that names the setting itself is still honoured.
$CLICKHOUSE_CLIENT -q "
    SET output_format_parquet_column_field_ids = {'missing': '1'};
    CREATE TABLE t_field_ids_definition (x Int64) ENGINE = File(Parquet, '${DATA_DIR}/definition.parquet') SETTINGS output_format_parquet_auto_assign_field_ids = 1;
    INSERT INTO t_field_ids_definition VALUES (4);
    SELECT * FROM t_field_ids_definition;
    DROP TABLE t_field_ids_definition;
"
dump_field_ids "${USER_FILES_PATH:?}/${DATA_DIR}/definition.parquet"

rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
