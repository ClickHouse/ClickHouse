#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro output format needs ENABLE_AVRO, which follows ENABLE_LIBRARIES=0 in the Fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP:?}/04668_${CLICKHOUSE_DATABASE:?}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

# The fd route needs clickhouse-local: TableFunctionFile only accepts a numeric first
# argument when getApplicationType() == LOCAL.
local_query() { ${CLICKHOUSE_LOCAL} --query "$1"; }

# Reports the error name when the insert is rejected, so a future unrelated failure
# cannot make the test pass vacuously.
insert_via_fd() {
    local target="$1" format="$2" query="$3"
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file(3, '${format}', 'c0 UInt8') ${query}" \
        3>>"${target}" 2>&1 >/dev/null | grep -oF 'CANNOT_APPEND_TO_FILE' | head -n 1
}

echo '-- 1. no-append format, non-empty target, zero rows: rejected, target untouched'
printf 'PREBYTES' > "${WORK_DIR}/zero_rows.avro"
insert_via_fd "${WORK_DIR}/zero_rows.avro" Avro 'SELECT 1 WHERE 0'
stat --format=%s "${WORK_DIR}/zero_rows.avro"

echo '-- 2. no-append format, non-empty target, with rows: rejected, target untouched'
printf 'PREBYTES' > "${WORK_DIR}/with_rows.avro"
insert_via_fd "${WORK_DIR}/with_rows.avro" Avro 'SELECT 1'
stat --format=%s "${WORK_DIR}/with_rows.avro"

echo '-- 3. no-append format, empty target: still written, and readable back'
: > "${WORK_DIR}/empty.avro"
insert_via_fd "${WORK_DIR}/empty.avro" Avro 'SELECT 7'
local_query "SELECT c0 FROM file('${WORK_DIR}/empty.avro', 'Avro')"

echo '-- 4. append-supporting format, non-empty target: still appends'
printf '{"c0":9}\n' > "${WORK_DIR}/append.jsonl"
insert_via_fd "${WORK_DIR}/append.jsonl" JSONEachRow 'SELECT 1'
local_query "SELECT c0 FROM file('${WORK_DIR}/append.jsonl', 'JSONEachRow') ORDER BY c0"

echo '-- 5. partitioned write into an existing partition: rejected, first partition intact'
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/part_{_partition_id}.avro', 'Avro', 'c0 UInt8') PARTITION BY c0 SELECT 3" 2>&1 \
    | grep -oF 'CANNOT_APPEND_TO_FILE' | head -n 1
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/part_{_partition_id}.avro', 'Avro', 'c0 UInt8') PARTITION BY c0 SELECT 3" 2>&1 \
    | grep -oF 'CANNOT_APPEND_TO_FILE' | head -n 1
local_query "SELECT c0 FROM file('${WORK_DIR}/part_3.avro', 'Avro')"

echo '-- 6. Parquet, non-empty target: rejected too, the guard is not Avro-specific'
printf 'PREBYTES' > "${WORK_DIR}/pq.parquet"
insert_via_fd "${WORK_DIR}/pq.parquet" Parquet 'SELECT 1'

echo '-- 7. engine_file_truncate_on_insert still replaces the file'
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/truncate.avro', 'Avro', 'c0 UInt8') SELECT 1"
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/truncate.avro', 'Avro', 'c0 UInt8') SELECT 2 SETTINGS engine_file_truncate_on_insert = 1"
local_query "SELECT c0 FROM file('${WORK_DIR}/truncate.avro', 'Avro')"

echo '-- 8. engine_file_allow_create_multiple_files still creates a new file'
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/multi.avro', 'Avro', 'c0 UInt8') SELECT 1"
local_query "INSERT INTO FUNCTION file('${WORK_DIR}/multi.avro', 'Avro', 'c0 UInt8') SELECT 2 SETTINGS engine_file_allow_create_multiple_files = 1"
local_query "SELECT c0 FROM file('${WORK_DIR}/multi*.avro', 'Avro') ORDER BY c0"

echo '-- 9. settings-dependent checker is consulted: appendable config accepted, non-appendable rejected'
printf 'x\n' > "${WORK_DIR}/custom.txt"
insert_via_fd "${WORK_DIR}/custom.txt" CustomSeparated "SELECT 1 SETTINGS format_custom_result_after_delimiter = ''"
local_query "SELECT count() FROM file('${WORK_DIR}/custom.txt', 'LineAsString')"
printf 'x\n' > "${WORK_DIR}/custom_reject.txt"
insert_via_fd "${WORK_DIR}/custom_reject.txt" CustomSeparated "SELECT 1 SETTINGS format_custom_result_after_delimiter = 'END'"
stat --format=%s "${WORK_DIR}/custom_reject.txt"

rm -rf "${WORK_DIR}"
