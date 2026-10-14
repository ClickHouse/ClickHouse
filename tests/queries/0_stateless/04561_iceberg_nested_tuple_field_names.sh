#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# `getIcebergType` recursed into tuple elements through `getNormalizedType`, which
# renames a named tuple's elements to "1", "2", ... So a tuple nested inside a
# tuple was published in the Iceberg schema with its inner field names replaced
# by positions. With the default Parquet format the INSERT then failed with
# `Code: 117 ... Column 'c0.outer.inner' has no field id in the Iceberg schema
# being written`; with 'Avro' the INSERT succeeded and the read failed with
# `Code: 10 ... Tuple doesn't have element with name 'inner'`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique on-disk path so the test is parallel-safe.
BASE_DIR="${USER_FILES_PATH}/t_${CLICKHOUSE_DATABASE}_${RANDOM}"
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}"

# Object names are database-scoped too: the stress job runs part of its threads
# against one shared database, where a repeat would otherwise hit TABLE_ALREADY_EXISTS.
PFX="t_${CLICKHOUSE_DATABASE}"

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --enable_nullable_tuple_type=1"

# The tags of the shapes exercised below, in one place so `cleanup` follows them.
TAGS="parquet avro depth3 multi array map unnamed"

# Drop the objects before removing their directories (a surviving table is re-attached
# with its data gone).
cleanup() {
    local query="" tag
    for tag in ${TAGS}; do
        query="${query} DROP TABLE IF EXISTS mv_${PFX}_${tag};"
    done
    for tag in ${TAGS}; do
        query="${query} DROP TABLE IF EXISTS src_${PFX}_${tag};"
        query="${query} DROP TABLE IF EXISTS dst_${PFX}_${tag};"
    done
    query="${query} DROP TABLE IF EXISTS added_${PFX};"
    ${CH} -q "${query}" > /dev/null 2>&1 || true
    rm -rf "${BASE_DIR}"
}
trap cleanup EXIT

# Innermost field name of the deepest struct in the latest metadata version.
innermost_name() {
    local latest
    latest=$(find "${BASE_DIR}/$1/metadata" -name 'v*.metadata.json' | sort -V | tail -1)
    python3 -c "
import json,sys
def deepest(fields, depth=0):
    best = (depth, None)
    for f in fields:
        t = f.get('type')
        if isinstance(t, dict) and t.get('type') == 'struct':
            cand = deepest(t['fields'], depth + 1)
        else:
            cand = (depth, f['name'])
        if cand[0] >= best[0]:
            best = cand
    return best
print(deepest(json.load(open(sys.argv[1]))['schemas'][-1]['fields'])[1])
" "${latest}"
}

# Write each shape through a materialized view (the write path that publishes the
# Iceberg schema) and read the value back, so the round trip is asserted end to end.
roundtrip() {
    local tag="$1" type="$2" insert="$3" format="$4"
    local engine="IcebergLocal('${BASE_DIR}/${tag}/'"
    [ -n "${format}" ] && engine="${engine}, '${format}'"
    ${CH} -q "
DROP TABLE IF EXISTS mv_${PFX}_${tag};
DROP TABLE IF EXISTS src_${PFX}_${tag};
DROP TABLE IF EXISTS dst_${PFX}_${tag};
CREATE TABLE src_${PFX}_${tag} (c0 ${type}) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dst_${PFX}_${tag} (c0 ${type}) ENGINE = ${engine});
CREATE MATERIALIZED VIEW mv_${PFX}_${tag} TO dst_${PFX}_${tag} AS SELECT c0 FROM src_${PFX}_${tag};
INSERT INTO src_${PFX}_${tag} ${insert};
SELECT '${tag}', toString(c0) FROM dst_${PFX}_${tag};
"
}

# 1-2. The reported shape, on both formats: Parquet fails at INSERT (Code 117),
#      Avro at read (Code 10).
roundtrip parquet  'Tuple(outer Tuple(inner UInt32))' 'VALUES (((7)))' ''
roundtrip avro     'Tuple(outer Tuple(inner UInt32))' 'VALUES (((7)))' 'Avro'
# 3. Depth 3: every level below the top one was renamed, not just the second.
roundtrip depth3   'Tuple(l1 Tuple(l2 Tuple(l3 UInt32)))' 'VALUES ((((7))))' 'Avro'
# 4. Several elements, so positional and real names disagree beyond the first.
roundtrip multi    'Tuple(o Tuple(i UInt32, j UInt32), p UInt32)' 'VALUES (((7,8),9))' 'Avro'
# 5-6. Array and Map recurse through the same tuple branch.
roundtrip array    'Tuple(x Array(Tuple(a UInt32)))' 'VALUES (([(7)]))' 'Avro'
roundtrip map      'Tuple(m Map(String, Tuple(a UInt32)))' \
    "SELECT tuple(map('k', tuple(7::UInt32)))::Tuple(m Map(String, Tuple(a UInt32)))" 'Avro'
# 7. Negative control: an unnamed inner tuple is already positional, so the
#    published names must stay "1", "2", ... and the round trip must keep working.
roundtrip unnamed  'Tuple(o Tuple(UInt32))' 'VALUES (((7)))' 'Avro'

# The invariant itself, not just its symptom: the innermost published field name
# must be the ClickHouse element name. Master wrote "1" here.
echo "avro innermost name: $(innermost_name avro)"
echo "unnamed innermost name: $(innermost_name unnamed)"

# ALTER ... ADD COLUMN publishes a schema through `generateAddColumnMetadata`, a
# second call site that a CREATE-only test would miss. Iceberg refuses adding a
# non-nullable column, hence Nullable.
${CH} -q "
DROP TABLE IF EXISTS added_${PFX};
CREATE TABLE added_${PFX} (id Int64) ENGINE = IcebergLocal('${BASE_DIR}/added/', 'Avro');
INSERT INTO added_${PFX} VALUES (1);
ALTER TABLE added_${PFX} ADD COLUMN c1 Nullable(Tuple(o Tuple(i UInt32)));
INSERT INTO added_${PFX} VALUES (2, ((7)));
SELECT 'added', id, toString(c1) FROM added_${PFX} ORDER BY id;
"
echo "added innermost name: $(innermost_name added)"
