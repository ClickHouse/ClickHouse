#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# A rename of a mapped column keeps the physical schema equal, so the schema equality check
# in the legacy Delta reader does not fire. The logical-to-physical map must still follow the
# latest `metaData` action; otherwise a read of the new logical name bypasses the
# UNSUPPORTED_METHOD guard and silently returns NULLs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# Commit 0 maps logical "id" to physical "col_x". Commit 1 renames "id" to "new_id",
# keeping the same physical name, without rewriting the data file.
DATA="part-00000-c000.snappy.parquet"
mkdir -p "$DIR/_delta_log"
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 \
    "INSERT INTO FUNCTION file('$DIR/$DATA', Parquet, 'col_x Int64') SELECT number FROM numbers(3)"

python3 - "$DIR" "$DATA" <<'EOF'
import json, os, sys

directory, data_path = sys.argv[1], sys.argv[2]

def mapped(name):
    return {"name": name, "type": "long", "nullable": True,
            "metadata": {"delta.columnMapping.physicalName": "col_x", "delta.columnMapping.id": 1}}

def meta_data(name):
    return {"metaData": {"id": "rename-test",
                         "format": {"provider": "parquet", "options": {}},
                         "schemaString": json.dumps({"type": "struct", "fields": [mapped(name)]}),
                         "partitionColumns": [],
                         "configuration": {"delta.columnMapping.mode": "name",
                                           "delta.columnMapping.maxColumnId": "1"},
                         "createdTime": 1600000000000}}

def write_log(version, actions):
    with open(os.path.join(directory, "_delta_log", "%020d.json" % version), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")

write_log(0, [
    {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
    meta_data("id"),
    {"add": {"path": data_path, "partitionValues": {},
             "size": os.path.getsize(os.path.join(directory, data_path)),
             "modificationTime": 1600000000000, "dataChange": True}},
])
write_log(1, [meta_data("new_id")])
EOF

echo '-- renamed logical name is rejected'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    CREATE TABLE t (new_id Nullable(Int64)) ENGINE = DeltaLakeLocal('$DIR');
    SELECT new_id FROM t ORDER BY new_id;
" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

echo '-- schema from the log: physical name still works'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 \
    "SELECT col_x FROM deltaLakeLocal('$DIR') ORDER BY col_x"

rm -rf "$DIR"
