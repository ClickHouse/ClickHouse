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
  "last-column-id": 40,
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
         "type": {"type": "map", "key-id": 21, "key": "string", "value-id": 22, "value": "int", "value-required": false}},
        {"id": 23, "name": "list_opt_struct_elem", "required": true,
         "type": {"type": "list", "element-id": 24, "element-required": false,
                  "element": {"type": "struct", "fields": [
                     {"id": 25, "name": "ex", "required": true, "type": "long"}]}}},
        {"id": 26, "name": "struct_opt_list_field", "required": true,
         "type": {"type": "struct", "fields": [
            {"id": 27, "name": "inner", "required": false,
             "type": {"type": "list", "element-id": 28, "element": "int", "element-required": true}}]}},
        {"id": 29, "name": "map_opt_struct_val", "required": true,
         "type": {"type": "map", "key-id": 30, "key": "string", "value-id": 31, "value-required": false,
                  "value": {"type": "struct", "fields": [
                     {"id": 32, "name": "vx", "required": true, "type": "long"}]}}}
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
# The `SETTINGS input_format_null_as_default = 1` clause pins the setting the read-back below needs.
# It belongs on the CREATE and not on the SELECT: a table engine freezes its format settings when it
# is created and ignores the session ones afterwards, so a per-statement pin on the SELECT would not
# reach the reader. Without it, stress runs that inject an old `compatibility` value get the pre-21.1
# default of `false`, under which a `["null", array]` union has no legal target (Array cannot be
# Nullable in ClickHouse) and the read-back throws.
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
        map_opt_val Map(String, Nullable(Int32)),
        list_opt_struct_elem Array(Tuple(ex Int64)),
        struct_opt_list_field Tuple(inner Array(Int32)),
        map_opt_struct_val Map(String, Tuple(vx Int64))
    ) ENGINE = IcebergLocal('${TEST_DIR}/', 'Avro') SETTINGS input_format_null_as_default = 1;
    INSERT INTO ${TABLE} VALUES (1, 2, [10, 20], map('k', 5), (7, 8), [99], map('r', 1), (3), [NULL, 4], map('z', NULL), [(41)], ([51, 52]), map('m', tuple(61)));
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
    """'optional' for a ["null", T] union, else the bare type name.

    A union must be exactly the 2-branch null-first form Iceberg optionality uses, so that a
    wrong arity or a swapped branch order is a failure rather than another 'optional' line.
    """
    if isinstance(avro_type, list):
        assert not any(isinstance(branch, list) for branch in avro_type), 'nested union'
        assert len(avro_type) == 2, 'expected 2 union branches, got %d' % len(avro_type)
        assert avro_type[0] == 'null', 'expected null-first union, got %r' % (avro_type[0],)
        inner = avro_type[1]
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

# An optional element/value/field nested inside a required container keeps its own union. Each of
# these paths is produced by exactly one call site in the writer, so one line moves per site.
by_name = {f['name']: f['type'] for f in schema['fields']}
print('list_opt_elem.element', describe(unwrap(by_name['list_opt_elem'])['items']), sep='\t')
print('map_opt_val.value', describe(unwrap(by_name['map_opt_val'])['values']), sep='\t')
print('opt_struct.sy', describe(unwrap(by_name['opt_struct'])['fields'][1]['type']), sep='\t')
print('list_opt_struct_elem.element', describe(unwrap(by_name['list_opt_struct_elem'])['items']), sep='\t')
print('struct_opt_list_field.inner', describe(unwrap(by_name['struct_opt_list_field'])['fields'][0]['type']), sep='\t')
print('map_opt_struct_val.value', describe(unwrap(by_name['map_opt_struct_val'])['values']), sep='\t')

# Iceberg field ids must survive the extra union level, at every depth.
print('field_ids', ','.join(str(f['field-id']) for f in schema['fields']), sep='\t')


def walk_ids(avro_type, path, out):
    """Collect `path=field-id` for every id-carrying record field, through every wrapper."""
    if isinstance(avro_type, list):
        for branch in avro_type:
            walk_ids(branch, path, out)
        return
    if not isinstance(avro_type, dict):
        return
    kind = avro_type['type']
    if kind == 'record':
        for field in avro_type['fields']:
            child = path + '.' + field['name'] if path else field['name']
            if 'field-id' in field:
                out.append((child, field['field-id']))
            walk_ids(field['type'], child, out)
    elif kind == 'array':
        walk_ids(avro_type['items'], path + '.element', out)
    elif kind == 'map':
        walk_ids(avro_type['values'], path + '.value', out)


nested_ids = []
walk_ids(schema, '', nested_ids)
for path, field_id in sorted(nested_ids):
    print('id', path, field_id, sep='\t')
PY

# Values round-trip unchanged.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE} FORMAT TSV"

# The union an optional container now carries needs input_format_null_as_default to land on a bare
# Array/Map, which cannot be Nullable in ClickHouse. Read the same file through `file()`, whose
# format settings do come from the query, to pin that behaviour instead of only describing it.
# `grep -c -m1` and not `grep -c`: the message arrives twice whenever the client asks the server for
# logs at `error` or a lower level (once as a log line, once as the exception), and that level comes
# from CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL, which is `warning` normally and `fatal` under stress.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data/*.avro', 'Avro', 'opt_list Array(Int32)') SETTINGS input_format_null_as_default = 0" 2>&1 \
    | grep -c -m1 "Cannot insert Avro Union"
${CLICKHOUSE_CLIENT} --query "SELECT opt_list FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data/*.avro', 'Avro', 'opt_list Array(Int32)') SETTINGS input_format_null_as_default = 1"
# A required container is a bare Avro array, so it reads at either value of the setting. A `grep`
# that matches nothing exits 1, and the runner fails a `.sh` test on any nonzero exit code, so the
# count has to be taken without letting that status become the script's own.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data/*.avro', 'Avro', 'req_list Array(Int32)') SETTINGS input_format_null_as_default = 0" 2>&1 \
    | grep -c -m1 "Cannot insert Avro Union" || true
