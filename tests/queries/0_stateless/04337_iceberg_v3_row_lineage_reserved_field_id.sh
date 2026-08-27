#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs pyarrow and experimental Iceberg writes.
#
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/107343
# The native (V3) Parquet reader threw ICEBERG_SPECIFICATION_VIOLATION when an Iceberg data file
# physically contains a reserved row-lineage column such as `_row_id` (field_id 2147483540).
# The Iceberg spec reserves field ids greater than 2147483447 and requires readers to ignore
# unrecognized reserved ids rather than failing, so reading the table must succeed and return the
# projected rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v3_row_lineage"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# Create a one-column Iceberg table. ClickHouse writes column `x` with parquet field_id 1, plus the
# Iceberg manifest/metadata describing only that column.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_v3_row_lineage (x Int32) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_v3_row_lineage (x) VALUES (1), (2), (3);
"

DATAFILE=$(ls "${ICEBERG_TABLE_PATH}"/data/*.parquet 2>/dev/null | head -1)

# Materialise the reserved row-lineage column `_row_id` (field_id 2147483540) into the data file,
# exactly as a spec-compliant Iceberg v3 writer does.
# Column `x` keeps its original field_id 1 so it still maps to the table column.
python3 - "$DATAFILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
x = pa.field("x", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
row_id = pa.field("_row_id", pa.int64(), nullable=True, metadata={b"PARQUET:field_id": b"2147483540"})
table = pa.table(
    {"x": pa.array([1, 2, 3], pa.int32()), "_row_id": pa.array([0, 1, 2], pa.int64())},
    schema=pa.schema([x, row_id]),
)
pq.write_table(table, path)
PY

# A spec-compliant reader ignores the reserved field id and returns the projected column.
${CLICKHOUSE_CLIENT} --query "SELECT x FROM icebergLocal('${ICEBERG_TABLE_PATH}') ORDER BY x;"

# A column with a field id outside of the table schema is skipped the same way, no matter whether
# the id is in the reserved range: it may also be a column that was dropped from the table, because
# `DROP COLUMN` is a metadata-only operation that leaves existing data files untouched.
ICEBERG_TABLE_PATH_UNMAPPED="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v3_unmapped"
rm -rf "${ICEBERG_TABLE_PATH_UNMAPPED}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_v3_unmapped (x Int32) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH_UNMAPPED}');
    INSERT INTO t_v3_unmapped (x) VALUES (1);
"

DATAFILE_UNMAPPED=$(ls "${ICEBERG_TABLE_PATH_UNMAPPED}"/data/*.parquet 2>/dev/null | head -1)

python3 - "$DATAFILE_UNMAPPED" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
# 2147483447 = Integer.MAX_VALUE - 200: the highest id a table may use, so NOT reserved.
# The unmapped column is named `x` too, as a column that was dropped and then re-added under the
# same name would be: matching by field id must win, so the query must return 1 and not 777.
extra = pa.field("x", pa.int64(), nullable=True, metadata={b"PARQUET:field_id": b"2147483447"})
x = pa.field("x", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
table = pa.Table.from_arrays(
    [pa.array([777], pa.int64()), pa.array([1], pa.int32())],
    schema=pa.schema([extra, x]),
)
pq.write_table(table, path)
PY

${CLICKHOUSE_CLIENT} --query "SELECT x FROM icebergLocal('${ICEBERG_TABLE_PATH_UNMAPPED}') ORDER BY x;"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_v3_row_lineage;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_v3_unmapped;"
rm -rf "${ICEBERG_TABLE_PATH}"
rm -rf "${ICEBERG_TABLE_PATH_UNMAPPED}"
