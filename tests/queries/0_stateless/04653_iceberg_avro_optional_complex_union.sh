#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: IcebergLocal and the Avro output format need USE_AVRO.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An Iceberg field carries an explicit `required` bit. A complex container (list/map/struct) is never
# wrapped in Nullable in the ClickHouse type, so an optional container used to be written as a bare
# Avro `array`/`map`/`record` -- indistinguishable from a required one. Assert on the schema the Avro
# file itself embeds: an optional field must be a `["null", T]` union, a required one must not be.
# The schema is hand-written because ClickHouse DDL cannot express an optional container (getIcebergType
# publishes required=true for Array/Map/Tuple), so this is the externally-authored-schema case.

TEST_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="t_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

rm -rf "${TEST_DIR}"
mkdir -p "${TEST_DIR}/metadata" "${TEST_DIR}/data"

cat > "${TEST_DIR}/metadata/v1.metadata.json" << EOF
{
  "format-version": 2,
  "table-uuid": "8f2a1c34-5b6d-4e7f-9a0b-1c2d3e4f5a6b",
  "location": "${TEST_DIR}",
  "last-updated-ms": 1700000000000,
  "last-column-id": 30,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "req_int", "required": true, "type": "int"},
        {"id": 2, "name": "opt_int", "required": false, "type": "int"},
        {"id": 3, "name": "opt_list", "required": false,
         "type": {"type": "list", "element-id": 10, "element": "int", "element-required": true}},
        {"id": 4, "name": "opt_map", "required": false,
         "type": {"type": "map", "key-id": 11, "key": "string", "value-id": 12, "value": "int", "value-required": true}},
        {"id": 5, "name": "opt_struct", "required": false,
         "type": {"type": "struct", "fields": [
            {"id": 13, "name": "sx", "required": true, "type": "long"},
            {"id": 14, "name": "sy", "required": false, "type": "long"}]}},
        {"id": 6, "name": "req_list", "required": true,
         "type": {"type": "list", "element-id": 15, "element": "int", "element-required": true}},
        {"id": 7, "name": "req_map", "required": true,
         "type": {"type": "map", "key-id": 16, "key": "string", "value-id": 17, "value": "int", "value-required": true}},
        {"id": 8, "name": "req_struct", "required": true,
         "type": {"type": "struct", "fields": [
            {"id": 18, "name": "rx", "required": true, "type": "long"}]}},
        {"id": 9, "name": "list_opt_elem", "required": true,
         "type": {"type": "list", "element-id": 19, "element": "int", "element-required": false}},
        {"id": 20, "name": "map_opt_val", "required": true,
         "type": {"type": "map", "key-id": 21, "key": "string", "value-id": 22, "value": "int", "value-required": false}}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": []
}
EOF

# IF NOT EXISTS attaches to the metadata already on disk instead of generating a new one, which is
# what keeps the hand-written optional-container schema in force.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE IF NOT EXISTS ${TABLE}
    (
        req_int Int32,
        opt_int Nullable(Int32),
        opt_list Array(Int32),
        opt_map Map(String, Int32),
        opt_struct Tuple(sx Int64, sy Nullable(Int64)),
        req_list Array(Int32),
        req_map Map(String, Int32),
        req_struct Tuple(rx Int64),
        list_opt_elem Array(Nullable(Int32)),
        map_opt_val Map(String, Nullable(Int32))
    ) ENGINE = IcebergLocal('${TEST_DIR}/', 'Avro');
    INSERT INTO ${TABLE} VALUES (1, 2, [10, 20], map('k', 5), (7, 8), [99], map('r', 1), (3), [NULL, 4], map('z', NULL));
"

# Nullability per field, straight out of the written file's `avro.schema` header entry. The reader
# maps union[NULL, array] back to Array(...), so DESCRIBE cannot tell the two apart -- the embedded
# schema is the only unambiguous witness.
python3 - "${TEST_DIR}" << 'PY'
import glob, json, re, sys

files = sorted(glob.glob(sys.argv[1] + '/data/*.avro'))
if not files:
    sys.exit('no Avro data file was written')

raw = open(files[0], 'rb').read()
start = raw.index(b'{', re.search(rb'avro\.schema', raw).end())
depth, end = 0, start
while True:
    char = raw[end:end + 1]
    if char == b'{':
        depth += 1
    elif char == b'}':
        depth -= 1
        if depth == 0:
            end += 1
            break
    end += 1
schema = json.loads(raw[start:end].decode())


def describe(avro_type):
    """'optional' for a ["null", T] union, else the bare type name."""
    if isinstance(avro_type, list):
        assert not any(isinstance(branch, list) for branch in avro_type), 'nested union'
        inner = [b for b in avro_type if b != 'null'][0]
        name = inner if isinstance(inner, str) else inner['type']
        return 'optional ' + name
    if isinstance(avro_type, str):
        return 'required ' + avro_type
    return 'required ' + avro_type['type']


def unwrap(avro_type):
    """The wrapped type of a ["null", T] union, or the type itself."""
    if isinstance(avro_type, list):
        return [b for b in avro_type if b != 'null'][0]
    return avro_type


for field in schema['fields']:
    print(field['name'], describe(field['type']), sep='\t')

# An optional element/value nested inside a required container keeps its own union.
by_name = {f['name']: f['type'] for f in schema['fields']}
print('list_opt_elem.element', describe(unwrap(by_name['list_opt_elem'])['items']), sep='\t')
print('map_opt_val.value', describe(unwrap(by_name['map_opt_val'])['values']), sep='\t')
print('opt_struct.sy', describe(unwrap(by_name['opt_struct'])['fields'][1]['type']), sep='\t')

# Iceberg field ids must survive the extra union level.
print('field_ids', ','.join(str(f['field-id']) for f in schema['fields']), sep='\t')
PY

# Values round-trip unchanged.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE} FORMAT TSV"
