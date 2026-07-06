#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/04501_data.csv ] && rm "${CLICKHOUSE_TMP}"/04501_data.csv
[ -e "${CLICKHOUSE_TMP}"/04501_data.csv.gz ] && rm "${CLICKHOUSE_TMP}"/04501_data.csv.gz

printf '1,A\n2,B\n' > "${CLICKHOUSE_TMP}"/04501_data.csv
gzip -k "${CLICKHOUSE_TMP}"/04501_data.csv

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_format_compression"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"

# Compressed data via stdin, next to a bare FORMAT clause (no FROM INFILE).
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'" < "${CLICKHOUSE_TMP}"/04501_data.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04501_data.csv "${CLICKHOUSE_TMP}"/04501_data.csv.gz

# Parser-only check: COMPRESSION next to a bare FORMAT (no FROM INFILE) is accepted and round-trips through formatting.
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION ''gzip''')"

# Parser-only check: COMPRESSION also round-trips for INSERT ... SELECT ... FROM input('CSV') FORMAT CSV (the input() branch).
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression SELECT * FROM input(''id UInt32, text String'') FORMAT CSV COMPRESSION ''gzip''')"

# Functional check: COMPRESSION works the same way through the input() table function as through a bare FORMAT clause.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"

printf '3,C\n4,D\n' > "${CLICKHOUSE_TMP}"/04501_data2.csv
gzip -k "${CLICKHOUSE_TMP}"/04501_data2.csv

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression SELECT * FROM input('id UInt32, text String') FORMAT CSV COMPRESSION 'gzip'" < "${CLICKHOUSE_TMP}"/04501_data2.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04501_data2.csv "${CLICKHOUSE_TMP}"/04501_data2.csv.gz

# Non-gzip compression method: COMPRESSION works with 'zstd' too, not just 'gzip'.
printf '7,G\n8,H\n' > "${CLICKHOUSE_TMP}"/04501_data3.csv
zstd -q -k -f "${CLICKHOUSE_TMP}"/04501_data3.csv -o "${CLICKHOUSE_TMP}"/04501_data3.csv.zst

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'zstd'" < "${CLICKHOUSE_TMP}"/04501_data3.csv.zst
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04501_data3.csv "${CLICKHOUSE_TMP}"/04501_data3.csv.zst

# clickhouse-local regression: bare FORMAT + COMPRESSION via stdin works the same way in clickhouse-local
# as in clickhouse-client (both share ClientBase::sendDataFrom for this path). A bare-FORMAT INSERT fed
# via stdin must be the last statement in its query text (true regardless of COMPRESSION -- the parser
# has no way to tell a mid-text ';' apart from insert data once FORMAT expects stdin), so CREATE and
# INSERT run in one invocation with a persistent --path, and SELECT runs in a second invocation.
# Uses MergeTree, not Memory, since Memory-engine data does not survive across clickhouse-local
# process restarts even with the same --path.
printf '9,I\n10,J\n' > "${CLICKHOUSE_TMP}"/04501_data4.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04501_data4.csv

rm -rf "${CLICKHOUSE_TMP}"/04501_local_path
mkdir -p "${CLICKHOUSE_TMP}"/04501_local_path

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04501_local_path --query "
CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'
" < "${CLICKHOUSE_TMP}"/04501_data4.csv.gz

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04501_local_path --query "SELECT * FROM test_insert_format_compression ORDER BY id"

rm -f "${CLICKHOUSE_TMP}"/04501_data4.csv "${CLICKHOUSE_TMP}"/04501_data4.csv.gz
rm -rf "${CLICKHOUSE_TMP}"/04501_local_path

# Negative check: COMPRESSION with an unknown method name after bare FORMAT is rejected with a clear error.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
echo '5,E' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'bogus'" 2>&1 | grep -c -o "Unknown compression method"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: COMPRESSION next to a bare FORMAT is rejected for data embedded inline in the query
# text (as opposed to piped via stdin), since inline data shares one buffer with any following query
# in a multiquery script and compressed streams have no unambiguous end marker for that boundary.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'
6,F
" 2>&1 | grep -c -o "only supported for data supplied via stdin"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Edge case: a real query follows the rejected inline-COMPRESSION INSERT in the same multiquery script.
# The true data boundary of compressed inline data can only be known by decompressing it, which is
# exactly what is being refused here so the rejection necessarily treats the rest of the script as
# still belonging to this (failed) INSERT, and the following query is not executed. This is documented
# in the error message; verify that behavior explicitly rather than assuming continuation.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --multiquery --ignore-error --query "
INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'
6,F
;
SELECT 'still alive';
" 2>&1 | grep -c -x "still alive" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_format_compression"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"
