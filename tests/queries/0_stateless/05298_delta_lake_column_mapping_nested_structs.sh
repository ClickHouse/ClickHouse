#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A Delta Lake table with column mapping must read struct fields by their physical names
# inside Array, Map and Nullable, not only in a top-level struct.
# https://github.com/ClickHouse/ClickHouse/issues/102571

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
mkdir -p "$DIR"/{mapped,partitioned,plain}/_delta_log

# The same rows are stored under physical names ("mapped") and under logical names ("plain").
MAPPED='`c-id` Int64,
    `c-it` Array(Tuple(phys_name Nullable(String), phys_price Nullable(Int32))),
    `c-pr` Map(String, Tuple(phys_x Nullable(Int32))),
    `c-st` Tuple(phys_c Nullable(Int32)),
    `c-s` Tuple(phys_arr Array(Tuple(phys_f Nullable(Int32), phys_g Tuple(phys_h Nullable(Int32))))),
    `c-aa` Array(Array(Tuple(phys_v Nullable(Int32)))),
    `c-m` Map(String, Array(Tuple(phys_w Nullable(Int32)))),
    `c-mk` Map(Tuple(phys_k Int32), Nullable(Int32))'
PLAIN='id Int64,
    items Array(Tuple(name Nullable(String), price Nullable(Int32))),
    props Map(String, Tuple(x Nullable(Int32))),
    st Tuple(c Nullable(Int32)),
    s Tuple(arr Array(Tuple(f Nullable(Int32), g Tuple(h Nullable(Int32))))),
    aa Array(Array(Tuple(v Nullable(Int32)))),
    m Map(String, Array(Tuple(w Nullable(Int32)))),
    mk Map(Tuple(k Int32), Nullable(Int32))'
ROWS="(1, [('hello', 42), ('world', 99)], {'k1': (7)}, (100), ([(1, (11)), (2, (12))]), [[(5)], [(6), (7)]], {'a': [(8)]}, {(1): 10}),
    (2, [('foo', 10)], {'k2': (8)}, (101), ([(3, (13))]), [[(9)]], {'b': [(10), (11)]}, {(2): 20})"

$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/mapped/data.parquet', Parquet, '$MAPPED') VALUES $ROWS;
    INSERT INTO FUNCTION file('$DIR/plain/data.parquet', Parquet, '$PLAIN') VALUES $ROWS;
"
cp "$DIR/mapped/data.parquet" "$DIR/partitioned/data.parquet"

python3 - "$DIR" <<'EOF'
import json, os, sys

directory = sys.argv[1]

def struct(*fields):
    return {"type": "struct", "fields": list(fields)}

def array(element):
    return {"type": "array", "elementType": element, "containsNull": True}

def map_(key, value):
    return {"type": "map", "keyType": key, "valueType": value, "valueContainsNull": True}

def schema(mapped, partitioned=False):
    ids = iter(range(1, 100))
    def f(name, type_, physical, nullable=True):
        metadata = {}
        if mapped:
            metadata = {"delta.columnMapping.id": next(ids), "delta.columnMapping.physicalName": physical}
        return {"name": name, "type": type_, "nullable": nullable, "metadata": metadata}

    fields = [
        f("id", "long", "c-id"),
        f("items", array(struct(f("name", "string", "phys_name"), f("price", "integer", "phys_price"))), "c-it"),
        f("props", map_("string", struct(f("x", "integer", "phys_x"))), "c-pr"),
    ]
    if partitioned:
        fields.append(f("p", "string", "c-p"))
    else:
        fields += [
            f("st", struct(f("c", "integer", "phys_c")), "c-st"),
            f("s", struct(f("arr", array(struct(
                f("f", "integer", "phys_f"),
                f("g", struct(f("h", "integer", "phys_h")), "phys_g"))), "phys_arr")), "c-s"),
            f("aa", array(array(struct(f("v", "integer", "phys_v")))), "c-aa"),
            f("m", map_("string", array(struct(f("w", "integer", "phys_w")))), "c-m"),
            f("mk", map_(struct(f("k", "integer", "phys_k", nullable=False)), "integer"), "c-mk"),
        ]
    return fields, next(ids) - 1

for table in ["mapped", "partitioned", "plain"]:
    fields, max_id = schema(table != "plain", table == "partitioned")
    if table == "plain":
        protocol = {"minReaderVersion": 1, "minWriterVersion": 2}
        configuration = {}
    else:
        protocol = {"minReaderVersion": 2, "minWriterVersion": 5}
        configuration = {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": str(max_id)}
    table_dir = os.path.join(directory, table)
    actions = [
        {"protocol": protocol},
        {"metaData": {"id": "102571-" + table,
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps(struct(*fields)),
                      "partitionColumns": ["p"] if table == "partitioned" else [],
                      "configuration": configuration, "createdTime": 1700000000000}},
        {"add": {"path": "data.parquet",
                 "partitionValues": {"c-p": "1"} if table == "partitioned" else {},
                 "size": os.path.getsize(os.path.join(table_dir, "data.parquet")),
                 "modificationTime": 1700000000000, "dataChange": True,
                 "stats": json.dumps({"numRecords": 2})}},
    ]
    with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")
EOF

$CLICKHOUSE_LOCAL -q "
    SELECT 'mapped';
    SELECT id, items, props, st, s, aa, m, mk FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
    SELECT 'mapped, array size';
    SELECT id, items.size0, length(items) FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
    SELECT 'mapped, array of structs inside a struct';
    SELECT id, s.arr FROM deltaLakeLocal('$DIR/mapped') ORDER BY id;
    SELECT 'mapped, declared Nullable structs';
    CREATE TABLE t_nullable
    (
        id Nullable(Int64),
        st Nullable(Tuple(c Nullable(Int32))),
        items Array(Nullable(Tuple(name Nullable(String), price Nullable(Int32))))
    )
    ENGINE = DeltaLakeLocal('$DIR/mapped');
    SELECT id, st, items FROM t_nullable ORDER BY id;
    SELECT 'mapped, partitioned';
    SELECT id, p, items, props FROM deltaLakeLocal('$DIR/partitioned') ORDER BY id;
    SELECT 'no column mapping';
    SELECT id, items, props, st, s, aa, m, mk FROM deltaLakeLocal('$DIR/plain') ORDER BY id;
"

rm -rf "$DIR"
