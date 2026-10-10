#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A Delta Lake table with column mapping can have a field whose name contains a dot, like `a.b`, next to a struct `a`
# with a field `b`. Each of them must read its own values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
for T in cols reversed backslash partitioned partitioned_by_dotted plain; do
    mkdir -p "$DIR/$T/_delta_log"
done
mkdir -p "$DIR/partitioned/c-p=x" "$DIR/partitioned_by_dotted/c-ab=x" "$DIR/partitioned_by_dotted/c-ab=y"

# Data files use the physical names.
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$DIR/cols/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-a\` Tuple(\`c-a-b\` Nullable(Int32)), \`c-ab\` Nullable(Int32),
     \`c-s\` Tuple(\`c-s-a\` Tuple(\`c-s-a-b\` Nullable(Int32)), \`c-s-ab\` Nullable(Int32)),
     \`c-o\` Tuple(\`c-o-s\` Tuple(\`c-o-s-a\` Tuple(\`c-o-s-a-b\` Nullable(Int32)), \`c-o-s-ab\` Nullable(Int32))),
     \`c-xy\` Nullable(Int32), \`c-d\` Tuple(\`c-d-xy\` Nullable(Int32))')
VALUES (1, (1), 2, ((1), 2), (((1), 2)), 5, (6)), (2, (3), 4, ((3), 4), (((3), 4)), 7, (8));
INSERT INTO FUNCTION file('$DIR/reversed/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-ab\` Nullable(Int32), \`c-a\` Tuple(\`c-a-b\` Nullable(Int32)),
     \`c-r\` Tuple(\`c-r-ab\` Nullable(Int32), \`c-r-a\` Tuple(\`c-r-a-b\` Nullable(Int32)))')
VALUES (1, 2, (1), (2, (1))), (2, 4, (3), (4, (3)));
INSERT INTO FUNCTION file('$DIR/backslash/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-a\` Tuple(\`c-a-b\` Nullable(Int32)), \`c-ab\` Nullable(Int32)')
VALUES (1, (1), 2), (2, (3), 4);
INSERT INTO FUNCTION file('$DIR/partitioned/c-p=x/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-a\` Tuple(\`c-a-b\` Nullable(Int32)), \`c-ab\` Nullable(Int32)')
VALUES (1, (1), 2), (2, (3), 4);
INSERT INTO FUNCTION file('$DIR/partitioned_by_dotted/c-ab=x/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-a\` Tuple(\`c-a-b\` Nullable(Int32))')
VALUES (1, (1));
INSERT INTO FUNCTION file('$DIR/partitioned_by_dotted/c-ab=y/data.parquet', Parquet,
    '\`c-id\` Int32, \`c-a\` Tuple(\`c-a-b\` Nullable(Int32))')
VALUES (2, (3));
INSERT INTO FUNCTION file('$DIR/plain/data.parquet', Parquet,
    'id Int32, \`x.y\` Nullable(Int32), d Tuple(\`x.y\` Nullable(Int32))')
VALUES (1, 5, (6)), (2, 7, (8));
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

def write_log(table, fields, files, partition_columns=(), mapped=True):
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
                      "schemaString": json.dumps(struct(*fields)), "partitionColumns": list(partition_columns),
                      "configuration": configuration, "createdTime": 1700000000000}},
    ]
    for path, partition_values, rows in files:
        actions.append({"add": {"path": path, "partitionValues": partition_values,
                                "size": os.path.getsize(os.path.join(table_dir, path)),
                                "modificationTime": 1700000000000, "dataChange": True,
                                "stats": json.dumps({"numRecords": rows})}})
    with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")
    max_id = 0

write_log("cols", [
    field("id", "integer", "c-id"),
    field("a", struct(field("b", "integer", "c-a-b")), "c-a"),
    field("a.b", "integer", "c-ab"),
    field("s", struct(
        field("a", struct(field("b", "integer", "c-s-a-b")), "c-s-a"),
        field("a.b", "integer", "c-s-ab")), "c-s"),
    field("o", struct(
        field("s", struct(
            field("a", struct(field("b", "integer", "c-o-s-a-b")), "c-o-s-a"),
            field("a.b", "integer", "c-o-s-ab")), "c-o-s")), "c-o"),
    field("x.y", "integer", "c-xy"),
    field("d", struct(field("x.y", "integer", "c-d-xy")), "c-d"),
], [("data.parquet", {}, 2)])

write_log("reversed", [
    field("id", "integer", "c-id"),
    field("a.b", "integer", "c-ab"),
    field("a", struct(field("b", "integer", "c-a-b")), "c-a"),
    field("r", struct(
        field("a.b", "integer", "c-r-ab"),
        field("a", struct(field("b", "integer", "c-r-a-b")), "c-r-a")), "c-r"),
], [("data.parquet", {}, 2)])

# A struct `a\` with a field `b` next to a column `a.b`: each must read its own values too.
write_log("backslash", [
    field("id", "integer", "c-id"),
    field("a\\", struct(field("b", "integer", "c-a-b")), "c-a"),
    field("a.b", "integer", "c-ab"),
], [("data.parquet", {}, 2)])

write_log("partitioned", [
    field("id", "integer", "c-id"),
    field("a", struct(field("b", "integer", "c-a-b")), "c-a"),
    field("a.b", "integer", "c-ab"),
    field("p", "string", "c-p"),
], [("c-p=x/data.parquet", {"c-p": "x"}, 2)], ["p"])

write_log("partitioned_by_dotted", [
    field("id", "integer", "c-id"),
    field("a", struct(field("b", "integer", "c-a-b")), "c-a"),
    field("a.b", "string", "c-ab"),
], [("c-ab=x/data.parquet", {"c-ab": "x"}, 1), ("c-ab=y/data.parquet", {"c-ab": "y"}, 1)], ["a.b"])

write_log("plain", [
    field("id", "integer"),
    field("x.y", "integer"),
    field("d", struct(field("x.y", "integer"))),
], [("data.parquet", {}, 2)], mapped=False)
EOF

# The engine predicate is disabled in the filtered query: it does not support dotted names yet (#118775).
# A subcolumn name that matches two fields, like s.`a.b` here, reads the first of them in the order of the table schema,
# which can be declared in a different order than the Delta log.
$CLICKHOUSE_LOCAL -q "
SELECT 'a and a.b';
SELECT id, a, \`a.b\` FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'a.b';
SELECT id, \`a.b\` FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'struct with a and a.b';
SELECT id, s FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'nested struct';
SELECT id, o FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'nested struct element';
SELECT id, o.s FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'dotted names without a collision';
SELECT id, \`x.y\`, d FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT id, d.\`x.y\` FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT 'declared schema';
CREATE TABLE declared (id Nullable(Int32), \`d.x.y\` Nullable(Int32), \`o.s\` Tuple(a Tuple(b Nullable(Int32)), \`a.b\` Nullable(Int32)))
    ENGINE = DeltaLakeLocal('$DIR/cols');
SELECT id, \`d.x.y\`, \`o.s\` FROM declared ORDER BY id;
SELECT 'a.b before a';
SELECT id, \`a.b\`, a, r FROM deltaLakeLocal('$DIR/reversed') ORDER BY id;
SELECT 'ambiguous subcolumn';
SELECT id, s.\`a.b\` FROM deltaLakeLocal('$DIR/cols') ORDER BY id;
SELECT id, r.\`a.b\` FROM deltaLakeLocal('$DIR/reversed') ORDER BY id;
SELECT 'ambiguous subcolumn in a declared schema';
CREATE TABLE declared_reordered (id Nullable(Int32), s Tuple(\`a.b\` Nullable(Int32), a Tuple(b Nullable(Int32))),
    \`o.s\` Tuple(\`a.b\` Nullable(Int32), a Tuple(b Nullable(Int32)))) ENGINE = DeltaLakeLocal('$DIR/cols');
SELECT id, s.\`a.b\`, \`o.s\`.\`a.b\` FROM declared_reordered ORDER BY id;
CREATE TABLE declared_reversed (id Nullable(Int32), r Tuple(a Tuple(b Nullable(Int32)), \`a.b\` Nullable(Int32)))
    ENGINE = DeltaLakeLocal('$DIR/reversed');
SELECT id, r.\`a.b\` FROM declared_reversed ORDER BY id;
SELECT 'a backslash in a name';
SELECT id, \`a\\\\\`, \`a.b\` FROM deltaLakeLocal('$DIR/backslash') ORDER BY id;
SELECT 'partitioned';
SELECT id, a, \`a.b\`, p FROM deltaLakeLocal('$DIR/partitioned') ORDER BY id;
SELECT 'partitioned by a.b';
SELECT id, a, \`a.b\` FROM deltaLakeLocal('$DIR/partitioned_by_dotted') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DIR/partitioned_by_dotted') WHERE \`a.b\` = 'y' ORDER BY id SETTINGS delta_lake_enable_engine_predicate = 0;
SELECT 'no column mapping';
SELECT id, \`x.y\`, d FROM deltaLakeLocal('$DIR/plain') ORDER BY id;
SELECT id, d.\`x.y\` FROM deltaLakeLocal('$DIR/plain') ORDER BY id;
"

rm -rf "$DIR"
