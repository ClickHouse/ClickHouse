#!/usr/bin/env bash

# A `columns_substreams.txt` with fewer columns than `columns.txt` detaches its part as broken and the
# table stays queryable: red if the mismatch is a logical error (aborts debug builds).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05331_columns_substreams_column_mismatch"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    CREATE TABLE t (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO t SELECT number, number FROM numbers(10);
" </dev/null

printf 'columns substreams version: 1\n1 columns:\n1 substreams for column `id`:\n\tid\n' \
    > "$(find "${WORKING_DIR}" -name columns_substreams.txt)"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    SELECT count() FROM t;
    SELECT reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't';
" </dev/null

rm -rf "${WORKING_DIR}"
