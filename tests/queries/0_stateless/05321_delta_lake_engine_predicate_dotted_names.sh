#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A filter on a Delta Lake column whose name has a dot, or on a subcolumn that is not a Delta field, returns the same
# rows as without the engine predicate, also in a change data feed query, and a filter on a nested field without dots
# in its names still skips files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
mkdir -p "$DIR/_delta_log"

# Data files use the physical names. The two files have disjoint values, so a skipped file is visible.
STRUCTURE='`c-id` Int32, `c-ab` Nullable(Int32), `c-a` Tuple(`c-a-b` Nullable(Int32)), `c-xy` Nullable(Int32),
    `c-d` Tuple(`c-d-xy` Nullable(Int32)), `c-s` Tuple(`c-s-x` Nullable(Int32)), `c-fg` Nullable(Bool)'
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$DIR/lo.parquet', Parquet, '$STRUCTURE')
    VALUES (1, 100, (1), 10, (20), (30), true), (2, 101, (2), 11, (21), (31), true);
INSERT INTO FUNCTION file('$DIR/hi.parquet', Parquet, '$STRUCTURE')
    VALUES (3, 300, (3), 12, (22), (32), false), (4, 301, (4), 13, (23), (33), false);
"

python3 - "$DIR" <<'EOF'
import json, os, sys

directory = sys.argv[1]

def field(name, type_, id_, physical):
    return {"name": name, "type": type_, "nullable": True,
            "metadata": {"delta.columnMapping.id": id_, "delta.columnMapping.physicalName": physical}}

def struct(*fields):
    return {"type": "struct", "fields": list(fields)}

# `a.b` is a column and also the path of the field `b` of the struct `a`. `x.y` and `f.g` are only columns, `d` has a
# field `x.y`, and `s.x` is a nested field without dots in its names.
schema = struct(
    field("id", "integer", 1, "c-id"),
    field("a.b", "integer", 2, "c-ab"),
    field("a", struct(field("b", "integer", 4, "c-a-b")), 3, "c-a"),
    field("x.y", "integer", 5, "c-xy"),
    field("d", struct(field("x.y", "integer", 7, "c-d-xy")), 6, "c-d"),
    field("s", struct(field("x", "integer", 9, "c-s-x")), 8, "c-s"),
    field("f.g", "boolean", 10, "c-fg"),
)

def values(id_, ab, a_b, xy, d_xy, s_x):
    return {"c-id": id_, "c-ab": ab, "c-a": {"c-a-b": a_b}, "c-xy": xy, "c-d": {"c-d-xy": d_xy}, "c-s": {"c-s-x": s_x}}

def add(path, min_values, max_values):
    null_count = {k: {kk: 0 for kk in v} if isinstance(v, dict) else 0 for k, v in min_values.items()}
    stats = {"numRecords": 2, "minValues": min_values, "maxValues": max_values, "nullCount": null_count}
    return {"add": {"path": path, "partitionValues": {}, "size": os.path.getsize(os.path.join(directory, path)),
                    "modificationTime": 1700000000000, "dataChange": True, "stats": json.dumps(stats)}}

actions = [
    {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
    {"metaData": {"id": "t", "format": {"provider": "parquet", "options": {}}, "schemaString": json.dumps(schema),
                  "partitionColumns": [], "createdTime": 1700000000000,
                  "configuration": {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "10"}}},
    add("lo.parquet", values(1, 100, 1, 10, 20, 30), values(2, 101, 2, 11, 21, 31)),
    add("hi.parquet", values(3, 300, 3, 12, 22, 32), values(4, 301, 4, 13, 23, 33)),
]
with open(os.path.join(directory, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in actions:
        log.write(json.dumps(action) + "\n")
EOF

# delta-kernel refuses a change data feed on a table with column mapping, so the feed is read from a second table
# without it.
mkdir -p "$DIR/cdf/_delta_log"
CDF_STRUCTURE='id Int32, `x.y` Nullable(Int32), s Tuple(x Nullable(Int32))'
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$DIR/cdf/data.parquet', Parquet, '$CDF_STRUCTURE') VALUES (1, 10, (20)), (2, 11, (21));
"

python3 - "$DIR/cdf" <<'EOF'
import json, os, sys

directory = sys.argv[1]

def field(name, type_):
    return {"name": name, "type": type_, "nullable": True, "metadata": {}}

schema = {"type": "struct", "fields": [
    field("id", "integer"), field("x.y", "integer"), field("s", {"type": "struct", "fields": [field("x", "integer")]})]}

actions = [
    {"protocol": {"minReaderVersion": 1, "minWriterVersion": 4}},
    {"metaData": {"id": "c", "format": {"provider": "parquet", "options": {}}, "schemaString": json.dumps(schema),
                  "partitionColumns": [], "createdTime": 1700000000000,
                  "configuration": {"delta.enableChangeDataFeed": "true"}}},
    {"add": {"path": "data.parquet", "partitionValues": {},
             "size": os.path.getsize(os.path.join(directory, "data.parquet")),
             "modificationTime": 1700000000000, "dataChange": True}},
]
with open(os.path.join(directory, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in actions:
        log.write(json.dumps(action) + "\n")
EOF

# Prints the matching ids and the number of data files read.
check() {
    echo "$1"
    $CLICKHOUSE_LOCAL -q "
        SELECT id FROM deltaLakeLocal('$DIR') WHERE $1 ORDER BY id;
        SELECT 'files: ' || toString(sumIf(value, event = 'DeltaLakeScannedFiles')) FROM system.events;"
}

check '`a.b` = 100'
check '`a.b` >= 100'
check '`x.y` = 12'
check 'd.`x.y` = 21'
check 'NOT `f.g`'
check 's.x.null = 0'
check 's.x = 32'
check 's.x = 32 AND `a.b` = 300'

# Prints the matching ids of a change data feed query.
check_cdf() {
    echo "cdf: $1"
    $CLICKHOUSE_LOCAL -q "
        SET delta_lake_snapshot_start_version = 0;
        SELECT id FROM deltaLakeLocal('$DIR/cdf') WHERE $1 ORDER BY id;"
}

check_cdf '`x.y` = 11'
check_cdf 's.x.null = 0'

rm -rf "$DIR"
