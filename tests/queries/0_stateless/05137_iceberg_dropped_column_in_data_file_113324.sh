#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option) and pyarrow.
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/113324
#
# `DROP COLUMN` is a metadata-only operation, so data files written before it keep the column with
# its field id. A reader projects by field id and simply does not select a file column that the
# read schema has no id for (https://iceberg.apache.org/spec/#column-projection), but the native
# (V3) Parquet reader threw ICEBERG_SPECIFICATION_VIOLATION instead, which made such tables
# unreadable. The dropped id is at or below `last-column-id` of the table metadata, so the reader
# can tell it apart from an id the table has never assigned, which is still reported.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `y` takes field id 2 and is then dropped, so the current schema holds only `x` (field id 1) while
# `last-column-id` stays 2: field id 2 is a dropped column, field id 3 was never assigned.
create_table_with_dropped_field_id_2() {
    local table=$1
    local table_dir=$2
    rm -rf "${table_dir}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${table} (x Int32) ENGINE = IcebergLocal('${table_dir}/');
        ALTER TABLE ${table} ADD COLUMN y Nullable(Int64);
        ALTER TABLE ${table} DROP COLUMN y;
        INSERT INTO ${table} (x) VALUES (1), (2);
    "
}

# Rewrite the single data file so that it also holds a column with `field_id`, as a file written
# before the column was dropped does. `ghost_name` is the name that column has in the file.
add_column_to_data_file() {
    local table_dir=$1
    local field_id=$2
    local ghost_name=$3
    python3 - "$(ls "${table_dir}"/data/*.parquet | head -1)" "${field_id}" "${ghost_name}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path, field_id, ghost_name = sys.argv[1], sys.argv[2].encode(), sys.argv[3]
ghost = pa.field(ghost_name, pa.int64(), nullable=True, metadata={b"PARQUET:field_id": field_id})
x = pa.field("x", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
# The dropped column comes first, so a reader that matches by position rather than by field id
# would pick it up as well.
table = pa.Table.from_arrays(
    [pa.array([777, 888], pa.int64()), pa.array([1, 2], pa.int32())],
    schema=pa.schema([ghost, x]),
)
pq.write_table(table, path)
PY
}

TABLE_DROPPED="t_${CLICKHOUSE_DATABASE}_dropped"
TABLE_DROPPED_DIR="${USER_FILES_PATH}/${TABLE_DROPPED}"
TABLE_SHADOWED="t_${CLICKHOUSE_DATABASE}_shadowed"
TABLE_SHADOWED_DIR="${USER_FILES_PATH}/${TABLE_SHADOWED}"
TABLE_UNASSIGNED="t_${CLICKHOUSE_DATABASE}_unassigned"
TABLE_UNASSIGNED_DIR="${USER_FILES_PATH}/${TABLE_UNASSIGNED}"

trap '
    rm -rf "${TABLE_DROPPED_DIR}" "${TABLE_SHADOWED_DIR}" "${TABLE_UNASSIGNED_DIR}"
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE_DROPPED};
        DROP TABLE IF EXISTS ${TABLE_SHADOWED};
        DROP TABLE IF EXISTS ${TABLE_UNASSIGNED};
    "
' EXIT

# The dropped column is not selected, and reading the table works.
create_table_with_dropped_field_id_2 "${TABLE_DROPPED}" "${TABLE_DROPPED_DIR}"
add_column_to_data_file "${TABLE_DROPPED_DIR}" 2 y
${CLICKHOUSE_CLIENT} --input_format_parquet_use_native_reader_v3=1 --query "SELECT x FROM ${TABLE_DROPPED} ORDER BY x;"

# A column dropped and later re-added under the same name keeps its old name in the old data files,
# so the name of a dropped column may coincide with the name of a column of the read schema. The
# projection is by field id, so the values of the dropped column must not be returned for `x`.
create_table_with_dropped_field_id_2 "${TABLE_SHADOWED}" "${TABLE_SHADOWED_DIR}"
add_column_to_data_file "${TABLE_SHADOWED_DIR}" 2 x
${CLICKHOUSE_CLIENT} --input_format_parquet_use_native_reader_v3=1 --query "SELECT x FROM ${TABLE_SHADOWED} ORDER BY x;"

# Conversely, field id 3 is above `last-column-id`, so the table cannot ever have written it: the
# file does not belong to this table, and reading it is still an error.
create_table_with_dropped_field_id_2 "${TABLE_UNASSIGNED}" "${TABLE_UNASSIGNED_DIR}"
add_column_to_data_file "${TABLE_UNASSIGNED_DIR}" 3 z
${CLICKHOUSE_CLIENT} --input_format_parquet_use_native_reader_v3=1 --query "SELECT x FROM ${TABLE_UNASSIGNED} ORDER BY x;" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1
