#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and python3 pyarrow.
#
# An Iceberg field marked `"required": false` whose type is a complex container
# (list/map/struct) was written to Parquet with `FieldRepetitionType::REQUIRED`, because
# Array/Map are never wrapped in `Nullable` in the ClickHouse type, so the optionality is
# not recoverable from the type and the Parquet writer never consulted the per-path Iceberg
# metadata the ORC writer already uses. The footer then contradicted the Iceberg schema published
# by the same commit, which is the mismatch this test pins.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_pq_opt"

trap 'rm -rf "${USER_FILES_PATH}/${BASE}"* 2>/dev/null' EXIT

# Reports the repetition type per leaf, plus the values as an external reader sees them.
# pyarrow is the oracle for the definition levels: unlike ParquetMetadata's per-column
# `max_definition_level`, it distinguishes a present-but-empty container (`[]`) from a null
# one (`None`), which is what pins the definition level of the new OPTIONAL group relative
# to the array's own level. ClickHouse's own reader normalizes a null container to an empty
# one, so a round-trip through ClickHouse cannot see that distinction.
# The `node` lines are what localize the level: a leaf's `maxdef` counts the nullable ancestors
# along its whole chain and therefore cannot say WHICH ancestor contributed, so it reads the same
# whether the container group or a node beneath it carries the OPTIONAL. Which node carries it is
# what has to match the published Iceberg schema.
read -r -d '' PROBE <<'PY'
import glob, sys
import pyarrow.parquet as pq
for fn in sorted(glob.glob(sys.argv[1] + '/data/*.parquet')):
    f = pq.ParquetFile(fn)
    schema = f.schema
    for i in range(len(schema)):
        c = schema.column(i)
        print('leaf %-24s maxdef=%d maxrep=%d' % (c.path, c.max_definition_level, c.max_repetition_level))
    sa = f.schema_arrow
    for name, get in (
            ('arr',           lambda: sa.field('arr')),
            ('arr.element',   lambda: sa.field('arr').type.field(0)),
            ('m',             lambda: sa.field('m')),
            ('m.key',         lambda: sa.field('m').type.key_field),
            ('m.value',       lambda: sa.field('m').type.item_field),
            ('st',            lambda: sa.field('st')),
            ('nst',           lambda: sa.field('nst')),
            ('nst.element',   lambda: sa.field('nst').type.field(0)),
            ('nq',            lambda: sa.field('nq')),
            ('nq.inner',      lambda: sa.field('nq').type.field('inner')),
            ('sc',            lambda: sa.field('sc'))):
        print('node %-16s optional=%s' % (name, get().nullable))
    table = f.read()
    for name in table.column_names:
        print('data %-4s %s' % (name, table.column(name).to_pylist()))
PY

# Marks the complex fields optional in a new schema version, the way another engine would.
# An optional top-level list/map is not producible by our own CREATE (getIcebergType returns
# required=true for Array/Map), so the metadata has to be authored externally. `nst` stays a
# required list whose *element* becomes optional: that nested case is what proves the dotted
# path is accumulated correctly rather than only matched at the top level.
read -r -d '' MARK_OPTIONAL <<'PY'
import glob, json, os, sys
md = sys.argv[1]
versions = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
                  key=lambda p: int(os.path.basename(p)[1:].split('.')[0]))
current = versions[-1]
n = int(os.path.basename(current)[1:].split('.')[0])
m = json.load(open(current))
schema = json.loads(json.dumps(m['schemas'][-1]))
schema['schema-id'] = len(m['schemas'])
for field in schema['fields']:
    if field['name'] in ('arr', 'm', 'st'):
        field['required'] = False
    if field['name'] == 'nst':
        field['type']['element-required'] = False
    if field['name'] == 'nq':
        # An optional container nested under a Nullable-owned struct: the struct's own path is
        # owned by the Nullable, but `nq.inner` is a distinct path that must be consulted.
        field['type']['fields'][0]['required'] = False
m['schemas'].append(schema)
m['current-schema-id'] = schema['schema-id']
m['metadata-log'] = m.get('metadata-log', []) + [
    {'timestamp-ms': m['last-updated-ms'], 'metadata-file': current}]
m['last-updated-ms'] += 1
json.dump(m, open(os.path.join(md, 'v%d.metadata.json' % (n + 1)), 'w'), indent=1)
open(os.path.join(md, 'version-hint.text'), 'w').write(str(n + 1))
PY

# `ns`/`nq` are declared `Nullable(Tuple(...))`, which is the one optional container a plain
# CREATE TABLE can express (getIcebergType maps Nullable(T) to required=false). They are
# carriers, not controls: the sink's header comes from the Iceberg schema, whose builder never
# wraps a container in Nullable (canBeInsideNullable() is false for Array/Map, and
# getComplexTypeFromObject returns a bare Tuple), so the writer receives a plain `Tuple` and
# the container reaches prepareColumnTuple rather than prepareColumnNullable. `nq.inner` also
# covers the nested case where a container sits below a struct that is itself optional.
create_and_fill() {
    local table="$1" path="$2" parallel="$3"
    ${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type 1 --query "
        CREATE TABLE ${table} (
            arr Array(Int32),
            m Map(String, Int32),
            st Tuple(a Int32),
            nst Array(Tuple(b Int32)),
            ns Nullable(Tuple(c Int32)),
            nq Nullable(Tuple(inner Array(Int32))),
            sc Nullable(Int32)
        ) ENGINE = IcebergLocal('${path}')
    " < /dev/null
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --async_insert 0 \
        --output_format_parquet_parallel_encoding "${parallel}" --max_threads 4 --query "
        INSERT INTO ${table} VALUES
            ([1, 2], {'k': 1}, (7), [(3)], (9), ([4]), 5),
            ([],     {},       (8), [],    NULL, NULL,  NULL)
    " < /dev/null
}

# Both encoders have to agree with the footer: the reader takes its max definition level from
# the schema while the writer takes the level bit width from the state it builds, so a fix
# applied to only one of the two data paths silently produces unreadable files.
for parallel in 0 1
do
    # --- half A: every container required in the Iceberg schema stays REQUIRED -------------
    TABLE_A="${BASE}_a${parallel}"
    PATH_A="${USER_FILES_PATH}/${TABLE_A}/"
    rm -rf "${PATH_A}" 2>/dev/null
    create_and_fill "${TABLE_A}" "${PATH_A}" "${parallel}"

    echo "--- half A: containers required in the Iceberg schema stay REQUIRED (parallel_encoding=${parallel}) ---"
    python3 -c "${PROBE}" "${PATH_A}"

    # --- half B: containers marked optional become OPTIONAL --------------------------------
    TABLE_B="${BASE}_b${parallel}"
    PATH_B="${USER_FILES_PATH}/${TABLE_B}/"
    rm -rf "${PATH_B}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type 1 --query "
        CREATE TABLE ${TABLE_B} (
            arr Array(Int32),
            m Map(String, Int32),
            st Tuple(a Int32),
            nst Array(Tuple(b Int32)),
            ns Nullable(Tuple(c Int32)),
            nq Nullable(Tuple(inner Array(Int32))),
            sc Nullable(Int32)
        ) ENGINE = IcebergLocal('${PATH_B}')
    " < /dev/null
    python3 -c "${MARK_OPTIONAL}" "${PATH_B}metadata"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --async_insert 0 \
        --output_format_parquet_parallel_encoding "${parallel}" --max_threads 4 --query "
        INSERT INTO TABLE FUNCTION icebergLocal('${PATH_B}') VALUES
            ([1, 2], {'k': 1}, (7), [(3)], (9), ([4]), 5),
            ([],     {},       (8), [],    NULL, NULL,  NULL)
    " < /dev/null

    # Expected against half A: arr/m gain a definition level, st.a goes 0 -> 1, the optional
    # struct element lifts nst.list.element.b, and nq.inner rises again because its own path is
    # marked optional too. Untouched: every repeated list/key_value level (maxrep), the map key
    # (always required per the Iceberg spec), and the optional scalar. The second row's [] / {}
    # must read back as empty containers rather than NULL, which is what pins the new group's
    # definition level against the array's own.
    echo "--- half B: optional containers become OPTIONAL (parallel_encoding=${parallel}) ---"
    python3 -c "${PROBE}" "${PATH_B}"

    echo "--- half B: values round-trip through ClickHouse (parallel_encoding=${parallel}) ---"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT arr, m, st, nst, ns, nq, sc FROM icebergLocal('${PATH_B}') ORDER BY sc NULLS LAST
    " < /dev/null

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE_A}; DROP TABLE ${TABLE_B}" < /dev/null
done
