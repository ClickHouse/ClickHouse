#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and Iceberg writes.

# Regression test for a malformed manifest list reaching the Iceberg writer carry-forward path.
# `clickhouse local` contains the pre-fix fatal signal to a short-lived subprocess.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_malformed_manifest_list_writer_${CLICKHOUSE_TEST_UNIQUE_NAME}"
LOCAL_PATH="${WORK_DIR}/db"
TABLE_ROOT="${WORK_DIR}/iceberg/t0"

rm -rf "${WORK_DIR}"
mkdir -p "${LOCAL_PATH}" "${TABLE_ROOT}"
trap 'rm -rf "${WORK_DIR}"' EXIT

${CLICKHOUSE_LOCAL} \
    --path "${LOCAL_PATH}" \
    --allow_insert_into_iceberg=1 \
    --multiquery \
    --query "
        CREATE TABLE t0 (x Int32) ENGINE = IcebergLocal('${TABLE_ROOT}/');
        INSERT INTO t0 VALUES (1), (2);
    " -- --user_files_path="${WORK_DIR}"

MANIFEST_LIST=$(find "${TABLE_ROOT}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f | sort | head -1)
if [ -z "${MANIFEST_LIST}" ]; then
    echo "manifest list not found"
    exit 1
fi

MALFORMED_MANIFEST_LIST_HEX=$(python3 - <<'PY'
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


schema = b'{"type":"array","items":"int"}'
sync = bytes.fromhex('7d4f6998c268758353f227bc9dba7b7e')
metadata = [(b'avro.codec', b'null'), (b'avro.schema', schema)]

header = bytearray(b'Obj\x01')
header += write_long(len(metadata))
for key, value in metadata:
    header += write_bytes(key) + write_bytes(value)
header += write_long(0) + sync

# One root object: an Avro array containing one int value, `[1]`.
datum = write_long(1) + write_long(1) + write_long(0)
block = write_long(1) + write_long(len(datum)) + datum + sync
print((bytes(header) + block).hex())
PY
)

echo '--- malformed manifest list is rejected cleanly ---'
output=$(
    ${CLICKHOUSE_LOCAL} \
        --path "${LOCAL_PATH}" \
        --allow_insert_into_iceberg=1 \
        --engine_file_truncate_on_insert=1 \
        --multiquery \
        --query "
            SELECT count() FROM t0 FORMAT Null;
            INSERT INTO FUNCTION file('${MANIFEST_LIST}', RawBLOB) SELECT unhex('${MALFORMED_MANIFEST_LIST_HEX}');
            INSERT INTO t0 VALUES (3);
        " -- --user_files_path="${WORK_DIR}" 2>&1
)
status=$?

if echo "${output}" | grep -qF 'ICEBERG_SPECIFICATION_VIOLATION'; then
    echo 'ICEBERG_SPECIFICATION_VIOLATION'
elif [ "${status}" -eq 139 ]; then
    echo 'SIGSEGV'
elif echo "${output}" | grep -qF 'Received signal'; then
    echo 'Received signal'
elif echo "${output}" | grep -qF 'Segmentation fault'; then
    echo 'Segmentation fault'
else
    echo "${output}" | grep -E -m1 'Code:|Exception|signal|Segmentation|Aborted' || echo "exit status ${status}"
fi

echo '--- stateless server is still alive ---'
${CLICKHOUSE_CLIENT} --query "SELECT 1"
