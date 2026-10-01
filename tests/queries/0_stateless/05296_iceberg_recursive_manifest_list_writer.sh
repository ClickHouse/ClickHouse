#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and Iceberg writes.

# A manifest list with a recursive Avro schema must be rejected by an INSERT into an Iceberg table, not crash.
# `clickhouse local` contains the pre-fix fatal signal to a short-lived subprocess.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_recursive_manifest_list_writer_${CLICKHOUSE_TEST_UNIQUE_NAME}"

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

# Writes an Avro container file with the given schema and, if a datum is given, one block holding it. The datum is hex, or
# `deep:N` for a row of the nullable recursive schema nested N levels deep.
write_avro()
{
    python3 - "$@" <<'PY'
import sys

path, schema, datum_spec = sys.argv[1], sys.argv[2].encode(), sys.argv[3]


def write_long(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)


def write_bytes(value):
    return write_long(len(value)) + value


sync = bytes.fromhex('7d4f6998c268758353f227bc9dba7b7e')
metadata = [(b'avro.codec', b'null'), (b'avro.schema', schema)]

data = bytearray(b'Obj\x01')
data += write_long(len(metadata))
for key, value in metadata:
    data += write_bytes(key) + write_bytes(value)
data += write_long(0) + sync

if datum_spec:
    if datum_spec.startswith('deep:'):
        datum = b'\x02' * int(datum_spec[5:]) + b'\x00'
    else:
        datum = bytes.fromhex(datum_spec)
    data += write_long(1) + write_long(len(datum)) + datum + sync

with open(path, 'wb') as f:
    f.write(bytes(data))
PY
}

run_case()
{
    local name=$1 schema=$2 datum_spec=$3
    local case_dir="${WORK_DIR}/${name}"
    local table_root="${case_dir}/iceberg/t0"
    mkdir -p "${case_dir}/db" "${table_root}"

    ${CLICKHOUSE_LOCAL} \
        --path "${case_dir}/db" \
        --allow_insert_into_iceberg=1 \
        --multiquery \
        --query "
            CREATE TABLE t0 (x Int32) ENGINE = IcebergLocal('${table_root}/');
            INSERT INTO t0 VALUES (1), (2);
        " -- --user_files_path="${case_dir}"

    local manifest_list
    manifest_list=$(find "${table_root}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f | sort | head -1)
    if [ -z "${manifest_list}" ]; then
        echo "manifest list not found"
        return
    fi

    write_avro "${case_dir}/crafted.avro" "${schema}" "${datum_spec}"

    # The SELECT caches the valid manifest list, so only the INSERT, which carries the list forward, reads the replaced file.
    local output status
    output=$(
        ${CLICKHOUSE_LOCAL} \
            --path "${case_dir}/db" \
            --allow_insert_into_iceberg=1 \
            --use_iceberg_metadata_files_cache=1 \
            --engine_file_truncate_on_insert=1 \
            --multiquery \
            --query "
                SELECT count() FROM t0 FORMAT Null;
                INSERT INTO FUNCTION file('${manifest_list}', RawBLOB) SELECT * FROM file('${case_dir}/crafted.avro', RawBLOB);
                INSERT INTO t0 VALUES (3);
            " -- --user_files_path="${case_dir}" 2>&1
    )
    status=$?

    if echo "${output}" | grep -qF 'nested deeper than 256 levels'; then
        echo 'nested deeper than 256 levels'
    elif echo "${output}" | grep -qF 'is missing required field'; then
        echo 'missing required field'
    elif echo "${output}" | grep -qF 'Code:'; then
        echo "${output}" | grep -m1 -F 'Code:'
    else
        echo "no exception, exit status ${status}"
    fi
}

# `S` appears twice, but as siblings, so the schema is not recursive. The datum is one row: a.x = 1, b.x = 2.
echo '--- repeated named record type that is not recursive ---'
run_case repeated '{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"S"}]}' '0204'

echo '--- recursive record ---'
run_case recursive '{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}}]}' ''

# The datum is one row whose `next` is null.
echo '--- recursive record behind a nullable union ---'
run_case nullable '{"type":"record","name":"A","fields":[{"name":"next","type":["null","A"]}]}' '00'

echo '--- recursive record behind a nullable union, nested 1000000 levels deep ---'
run_case deep '{"type":"record","name":"A","fields":[{"name":"next","type":["null","A"]}]}' 'deep:1000000'

echo '--- stateless server is still alive ---'
${CLICKHOUSE_CLIENT} --query "SELECT 1"
