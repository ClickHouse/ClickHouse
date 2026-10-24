#!/usr/bin/env bash

# A power loss can leave a file that was never `fsync`ed at length zero while the rest of the part
# survives, and neither `columns.txt` nor `metadata_version.txt` is covered by the part checksums.
# An absent one of those two files is handled safely (the column list is regenerated from the table
# metadata, the version falls back to the table's), but an empty one used to fail to parse, which
# detached the whole part as broken and dropped its rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05212_zero_length_part_metadata_file"

run_case()
{
    local file_name="$1"
    rm -rf "${WORKING_DIR}"
    mkdir -p "${WORKING_DIR}"

    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
        CREATE TABLE t (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
        INSERT INTO t SELECT number, number + 1 FROM numbers(500);
        SELECT 'before ${file_name}', count(), sum(val) FROM t;
    " </dev/null

    find "${WORKING_DIR}" -name "${file_name}" -exec truncate -s 0 {} \;

    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
        SELECT 'after ${file_name}', count(), sum(val) FROM t;
        SELECT 'detached parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't';
    " </dev/null

    rm -rf "${WORKING_DIR}"
}

run_case columns.txt
run_case metadata_version.txt
