#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/121641
# A column that the Delta table schema no longer declares, while a ClickHouse table over it
# still lists it, must raise a catchable schema-drift error naming the column. Without
# delta.columnMapping the error used to be an internal NOT_FOUND_COLUMN_IN_BLOCK.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"

trap 'rm -rf "${TABLE_PATH}"; ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

mkdir -p "${TABLE_PATH}/_delta_log"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${TABLE_PATH}/v0.parquet', Parquet, 'c0 Int64, c1 Int64')
    SELECT number, number * 10 FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${TABLE_PATH}/v1.parquet', Parquet, 'c0 Int64')
    SELECT number FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1"

python3 - "$TABLE_PATH" <<'EOF'
import json, os, sys

lake = sys.argv[1]
column = lambda name: {"name": name, "type": "long", "nullable": True, "metadata": {}}

def meta(names):
    # An empty "configuration" leaves delta.columnMapping disabled.
    return {"metaData": {"id": "121641", "format": {"provider": "parquet", "options": {}},
                         "schemaString": json.dumps({"type": "struct",
                                                     "fields": [column(n) for n in names]}),
                         "partitionColumns": [], "configuration": {},
                         "createdTime": 1600000000000}}

def add(path):
    return {"add": {"path": path, "partitionValues": {},
                    "size": os.path.getsize(os.path.join(lake, path)),
                    "modificationTime": 1600000000000, "dataChange": True,
                    "stats": json.dumps({"numRecords": 3})}}

def write(name, actions):
    with open(os.path.join(lake, name), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")

# Version 0 declares c0 and c1. Version 1 drops c1, the way restoring a Delta table to a
# version older than an ADD COLUMNS does. It is staged outside _delta_log so that the table
# below is created while c1 still exists.
write("_delta_log/00000000000000000000.json",
      [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
       meta(["c0", "c1"]), add("v0.parquet")])
write("v1.json",
      [meta(["c0"]),
       {"remove": {"path": "v0.parquet", "deletionTimestamp": 1600000001000,
                   "dataChange": True}},
       add("v1.parquet")])
EOF

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = DeltaLakeLocal('${TABLE_PATH}')"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${TABLE} ORDER BY c0"

mv "${TABLE_PATH}/v1.json" "${TABLE_PATH}/_delta_log/00000000000000000001.json"

# The client forwards the server's own log record for the failed query, so each fragment
# reaches stderr twice.
DRIFT_ERROR=$($CLICKHOUSE_CLIENT -q "SELECT * FROM ${TABLE} ORDER BY c0" 2>&1)
echo "${DRIFT_ERROR}" | grep -m1 -oF "INCORRECT_DATA"
echo "${DRIFT_ERROR}" | grep -m1 -oF "Column c1 is not present in the DeltaLake table schema"
$CLICKHOUSE_CLIENT -q "SELECT c0 FROM ${TABLE} ORDER BY c0"
