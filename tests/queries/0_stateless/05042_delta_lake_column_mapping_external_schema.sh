#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# The legacy Delta reader (allow_delta_kernel_rs = 0) does not translate logical column names to
# the physical ones of a column-mapped table. When the column list comes from outside the log
# - a DataLakeCatalog database, or an explicit CREATE TABLE list - it holds logical names, and
# reading them silently returned all NULLs. Now such a read fails with UNSUPPORTED_METHOD.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# "mapped" has logical columns id/p stored under physical names col_iiii/col_pppp.
# "plain" is the same table without column mapping.
MAPPED_DATA="col_pppp=1/part-00000-c000.snappy.parquet"
PLAIN_DATA="p=1/part-00000-c000.snappy.parquet"
mkdir -p "$DIR/mapped/_delta_log" "$DIR/mapped/col_pppp=1" "$DIR/plain/_delta_log" "$DIR/plain/p=1"
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 --multiquery "
    INSERT INTO FUNCTION file('$DIR/mapped/$MAPPED_DATA', Parquet, 'col_iiii Int64') SELECT number FROM numbers(3);
    INSERT INTO FUNCTION file('$DIR/plain/$PLAIN_DATA', Parquet, 'id Int64') SELECT number FROM numbers(3);
"

python3 - "$DIR" "$MAPPED_DATA" "$PLAIN_DATA" <<'EOF'
import json, os, sys

directory, mapped_data, plain_data = sys.argv[1], sys.argv[2], sys.argv[3]

def write_log(table, fields, configuration, data_path, partition_values):
    table_dir = os.path.join(directory, table)
    with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in [
            {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
            {"metaData": {"id": table,
                          "format": {"provider": "parquet", "options": {}},
                          "schemaString": json.dumps({"type": "struct", "fields": fields}),
                          "partitionColumns": ["p"],
                          "configuration": configuration,
                          "createdTime": 1600000000000}},
            {"add": {"path": data_path, "partitionValues": partition_values,
                     "size": os.path.getsize(os.path.join(table_dir, data_path)),
                     "modificationTime": 1600000000000, "dataChange": True}},
        ]:
            log.write(json.dumps(action) + "\n")

mapped = lambda name, type_, physical, id_: {
    "name": name, "type": type_, "nullable": True,
    "metadata": {"delta.columnMapping.physicalName": physical, "delta.columnMapping.id": id_}}
write_log("mapped",
          [mapped("id", "long", "col_iiii", 1), mapped("p", "string", "col_pppp", 2)],
          {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "2"},
          mapped_data, {"col_pppp": "1"})

field = lambda name, type_: {"name": name, "type": type_, "nullable": True, "metadata": {}}
write_log("plain",
          [field("id", "long"), field("p", "string")],
          {}, plain_data, {"p": "1"})
EOF

echo '-- schema from the log: physical names, unchanged behaviour'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    DESCRIBE TABLE deltaLakeLocal('$DIR/mapped');
    SELECT col_iiii, col_pppp FROM deltaLakeLocal('$DIR/mapped') ORDER BY col_iiii;
" | cut -f1,2

echo '-- schema supplied externally: logical names are rejected'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    CREATE TABLE t (id Nullable(Int64), p Nullable(String)) ENGINE = DeltaLakeLocal('$DIR/mapped');
    SELECT id, p FROM t ORDER BY id;
" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

echo '-- schema supplied externally: physical names still work'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    CREATE TABLE t (col_iiii Nullable(Int64), col_pppp Nullable(String)) ENGINE = DeltaLakeLocal('$DIR/mapped');
    SELECT col_iiii, col_pppp FROM t ORDER BY col_iiii;
"

echo '-- control: no column mapping, both schema sources'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    SELECT id, p FROM deltaLakeLocal('$DIR/plain') ORDER BY id;
    CREATE TABLE t (id Nullable(Int64), p Nullable(String)) ENGINE = DeltaLakeLocal('$DIR/plain');
    SELECT id, p FROM t ORDER BY id;
"

rm -rf "$DIR"
