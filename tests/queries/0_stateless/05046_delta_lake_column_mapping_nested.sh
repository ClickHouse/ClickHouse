#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# `delta.columnMapping.physicalName` can appear on nested struct fields too, not only on
# top-level columns. The legacy Delta reader (allow_delta_kernel_rs = 0) keeps logical nested
# names in the exposed schema, so reading such a column silently returned NULLs for the renamed
# fields - through both the log-derived schema and an external column list. Now it fails with
# UNSUPPORTED_METHOD. Columns whose nested names are not renamed stay readable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# The data file uses physical names everywhere:
#   s1 -> col_s1, its nested field a is not renamed;
#   s2 -> col_s2, its nested field inner -> col_inner;
#   arr is not renamed, but the struct inside its elements has x -> col_x.
DATA="part-00000-c000.snappy.parquet"
mkdir -p "$DIR/_delta_log"
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 "
    INSERT INTO FUNCTION file(
        '$DIR/$DATA', Parquet,
        'col_s1 Tuple(a Nullable(Int64)), col_s2 Tuple(col_inner Nullable(Int64)), arr Array(Tuple(col_x Nullable(Int64)))')
    SELECT tuple(number), tuple(number * 10), [tuple(number * 100)] FROM numbers(3)
"

python3 - "$DIR" "$DATA" <<'EOF'
import json, os, sys

directory, data_path = sys.argv[1], sys.argv[2]

def fld(name, type_, physical, id_):
    return {"name": name, "type": type_, "nullable": True,
            "metadata": {"delta.columnMapping.physicalName": physical, "delta.columnMapping.id": id_}}

def struct(fields):
    return {"type": "struct", "fields": fields}

def array(element):
    return {"type": "array", "elementType": element, "containsNull": True}

fields = [
    fld("s1", struct([fld("a", "long", "a", 2)]), "col_s1", 1),
    fld("s2", struct([fld("inner", "long", "col_inner", 4)]), "col_s2", 3),
    fld("arr", array(struct([fld("x", "long", "col_x", 6)])), "arr", 5),
]

with open(os.path.join(directory, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {"metaData": {"id": "nested-mapping-test",
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps({"type": "struct", "fields": fields}),
                      "partitionColumns": [],
                      "configuration": {"delta.columnMapping.mode": "name",
                                        "delta.columnMapping.maxColumnId": "6"},
                      "createdTime": 1600000000000}},
        {"add": {"path": data_path, "partitionValues": {},
                 "size": os.path.getsize(os.path.join(directory, data_path)),
                 "modificationTime": 1600000000000, "dataChange": True}},
    ]:
        log.write(json.dumps(action) + "\n")
EOF

echo '-- schema from the log: physical top-level names, logical nested names'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 "DESCRIBE TABLE deltaLakeLocal('$DIR')" | cut -f1,2

echo '-- nested names not renamed: readable'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 "SELECT col_s1 FROM deltaLakeLocal('$DIR') ORDER BY col_s1"

echo '-- renamed nested field: rejected'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 "SELECT col_s2 FROM deltaLakeLocal('$DIR')" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

echo '-- renamed field inside array elements: rejected'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 "SELECT arr FROM deltaLakeLocal('$DIR')" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

echo '-- external schema with the logical nested name: rejected'
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs=0 --multiquery "
    CREATE TABLE t (col_s2 Tuple(inner Nullable(Int64))) ENGINE = DeltaLakeLocal('$DIR');
    SELECT col_s2 FROM t;
" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

rm -rf "$DIR"
