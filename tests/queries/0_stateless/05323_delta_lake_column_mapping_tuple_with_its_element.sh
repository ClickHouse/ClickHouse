#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A Delta Lake table with column mapping can read a struct together with its fields.
# https://github.com/ClickHouse/ClickHouse/issues/123811

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
for T in mapped plain json overlap; do
    mkdir -p "$DIR/$T/_delta_log"
done

# Data files use the physical names.
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$DIR/mapped/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-t\` Tuple(\`c-t-x\` Nullable(Int32), \`c-t-n\` Tuple(\`c-t-n-y\` Nullable(Int32)))')
VALUES (1, (10, (100))), (2, (20, (200))), (3, (NULL, (300)));
INSERT INTO FUNCTION file('$DIR/plain/data.parquet', Parquet,
    'id Int32, t Tuple(x Nullable(Int32), n Tuple(y Nullable(Int32)))')
VALUES (1, (10, (100))), (2, (20, (200))), (3, (NULL, (300)));
INSERT INTO FUNCTION file('$DIR/json/data.parquet', Parquet, '\`c-id\` Int32, \`c-t\` Tuple(\`c-t-j\` Nullable(String))')
VALUES (1, ('{\"a\":1,\"b\":2}')), (2, ('{\"a\":3,\"b\":4}'));
INSERT INTO FUNCTION file('$DIR/overlap/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-t\` Tuple(\`c-t-x\` Nullable(Int32), \`c-t-n\` Tuple(y Nullable(Int32), \`c-t-n-z\` Nullable(Int32)))')
VALUES (1, (10, (100, 1000))), (2, (20, (200, 2000)));
"

python3 - "$DIR" <<'EOF'
import json, os, sys

directory = sys.argv[1]
max_id = 0

def field(name, type_, physical=None):
    global max_id
    metadata = {}
    if physical is not None:
        max_id += 1
        metadata = {"delta.columnMapping.id": max_id, "delta.columnMapping.physicalName": physical}
    return {"name": name, "type": type_, "nullable": True, "metadata": metadata}

def struct(*fields):
    return {"type": "struct", "fields": list(fields)}

def write_log(table, fields, rows, mapped=True):
    global max_id
    table_dir = os.path.join(directory, table)
    if mapped:
        protocol = {"minReaderVersion": 2, "minWriterVersion": 5}
        configuration = {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": str(max_id)}
    else:
        protocol = {"minReaderVersion": 1, "minWriterVersion": 2}
        configuration = {}
    actions = [
        {"protocol": protocol},
        {"metaData": {"id": table, "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps(struct(*fields)), "partitionColumns": [],
                      "configuration": configuration, "createdTime": 1700000000000}},
        {"add": {"path": "data.parquet", "partitionValues": {},
                 "size": os.path.getsize(os.path.join(table_dir, "data.parquet")),
                 "modificationTime": 1700000000000, "dataChange": True,
                 "stats": json.dumps({"numRecords": rows})}},
    ]
    with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")
    max_id = 0

write_log("mapped", [
    field("id", "integer", "c-id"),
    field("t", struct(
        field("x", "integer", "c-t-x"),
        field("n", struct(field("y", "integer", "c-t-n-y")), "c-t-n")), "c-t"),
], 3)

write_log("plain", [
    field("id", "integer"),
    field("t", struct(field("x", "integer"), field("n", struct(field("y", "integer"))))),
], 3, mapped=False)

write_log("json", [
    field("id", "integer", "c-id"),
    field("t", struct(field("j", "string", "c-t-j")), "c-t"),
], 2)

write_log("overlap", [
    field("id", "integer", "c-id"),
    field("t", struct(
        field("x", "integer", "c-t-x"),
        field("n", struct(field("y", "integer", "y"), field("z", "integer", "c-t-n-z")), "c-t-n")), "c-t"),
], 2)
EOF

# The declared schema lists the fields of `t` in a different order than the Delta log.
# In `typed` the JSON path `b` is a typed path, in `dynamic` it is a dynamic one.
# In `overlap` the field `y` keeps its logical name as its physical name, as after enabling column mapping on an existing table.
$CLICKHOUSE_LOCAL -q "
SELECT 'struct and its field';
SELECT id, t, t.x FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
SELECT 'star and a field';
SELECT *, t.x FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
SELECT 'filter and order by a field';
SELECT t FROM deltaLakeLocal('$DIR/mapped') WHERE t.x = 20;
SELECT id FROM deltaLakeLocal('$DIR/mapped') ORDER BY t.x DESC NULLS LAST, t;
SELECT 'null map of a field';
SELECT id, t, t.x.null FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
SELECT 'nested struct and its field';
SELECT id, t.n, t.n.y FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
SELECT id, t, t.n.y FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
SELECT 'declared schema';
CREATE TABLE declared (id Nullable(Int32), t Tuple(n Tuple(y Nullable(Int32)), x Nullable(Int32)))
    ENGINE = DeltaLakeLocal('$DIR/mapped');
SELECT id, t, t.x, t.n.y FROM declared ORDER BY id;
SELECT 'no column mapping';
SELECT id, t, t.x, t.x.null, t.n.y FROM deltaLakeLocal('$DIR/plain') ORDER BY id;
SELECT 'typed JSON path';
CREATE TABLE typed (id Nullable(Int32), t Tuple(j JSON(a Int64, b Int64))) ENGINE = DeltaLakeLocal('$DIR/json');
SELECT id, t, t.j.b FROM typed ORDER BY id;
SELECT 'dynamic JSON path';
CREATE TABLE dynamic (id Nullable(Int32), t Tuple(j JSON(a Int64))) ENGINE = DeltaLakeLocal('$DIR/json');
SELECT id, t, t.j.b FROM dynamic ORDER BY id;
SELECT 'partly renamed struct and the struct';
SELECT id, t, t.n FROM deltaLakeLocal('$DIR/overlap') ORDER BY id;
"

rm -rf "$DIR"
