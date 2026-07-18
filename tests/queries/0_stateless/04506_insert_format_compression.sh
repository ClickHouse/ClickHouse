#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/04506_data.csv ] && rm "${CLICKHOUSE_TMP}"/04506_data.csv
[ -e "${CLICKHOUSE_TMP}"/04506_data.csv.gz ] && rm "${CLICKHOUSE_TMP}"/04506_data.csv.gz

printf '1,A\n2,B\n' > "${CLICKHOUSE_TMP}"/04506_data.csv
gzip -k "${CLICKHOUSE_TMP}"/04506_data.csv

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_format_compression"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"

# Compressed data via stdin, next to a bare FORMAT clause (no FROM INFILE).
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'" < "${CLICKHOUSE_TMP}"/04506_data.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data.csv "${CLICKHOUSE_TMP}"/04506_data.csv.gz

# Parser-only check: COMPRESSION next to a bare FORMAT (no FROM INFILE) is accepted and round-trips through formatting.
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION ''gzip''')"

# Parser-only check: COMPRESSION also round-trips for INSERT ... SELECT ... FROM input('CSV') FORMAT CSV (the input() branch).
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression SELECT * FROM input(''id UInt32, text String'') FORMAT CSV COMPRESSION ''gzip''')"

# Functional check: COMPRESSION works the same way through the input() table function as through a bare FORMAT clause.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"

printf '3,C\n4,D\n' > "${CLICKHOUSE_TMP}"/04506_data2.csv
gzip -k "${CLICKHOUSE_TMP}"/04506_data2.csv

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression SELECT * FROM input('id UInt32, text String') FORMAT CSV COMPRESSION 'gzip'" < "${CLICKHOUSE_TMP}"/04506_data2.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data2.csv "${CLICKHOUSE_TMP}"/04506_data2.csv.gz

# Non-gzip compression method: COMPRESSION works with 'zstd' too, not just 'gzip'.
printf '7,G\n8,H\n' > "${CLICKHOUSE_TMP}"/04506_data3.csv
zstd -q -k -f "${CLICKHOUSE_TMP}"/04506_data3.csv -o "${CLICKHOUSE_TMP}"/04506_data3.csv.zst

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'zstd'" < "${CLICKHOUSE_TMP}"/04506_data3.csv.zst
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data3.csv "${CLICKHOUSE_TMP}"/04506_data3.csv.zst

# COMPRESSION 'auto' on the bare-FORMAT stdin path must reuse the compression already
# auto-detected from the redirected stdin file descriptor's name, not silently become a no-op.
printf '15,O\n16,P\n' > "${CLICKHOUSE_TMP}"/04506_data6.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data6.csv

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'auto'" < "${CLICKHOUSE_TMP}"/04506_data6.csv.gz
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data6.csv "${CLICKHOUSE_TMP}"/04506_data6.csv.gz

# clickhouse-local regression: bare FORMAT + COMPRESSION via stdin works the same way in clickhouse-local
# as in clickhouse-client (both share ClientBase::sendDataFrom for this path). A bare-FORMAT INSERT fed
# via stdin must be the last statement in its query text (true regardless of COMPRESSION -- the parser
# has no way to tell a mid-text ';' apart from insert data once FORMAT expects stdin), so CREATE and
# INSERT run in one invocation with a persistent --path, and SELECT runs in a second invocation.
# Uses MergeTree, not Memory, since Memory-engine data does not survive across clickhouse-local
# process restarts even with the same --path.
printf '9,I\n10,J\n' > "${CLICKHOUSE_TMP}"/04506_data4.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data4.csv

rm -rf "${CLICKHOUSE_TMP}"/04506_local_path
mkdir -p "${CLICKHOUSE_TMP}"/04506_local_path

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path --query "
CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_insert_format_compression FORMAT CSV COMPRESSION 'gzip'
" < "${CLICKHOUSE_TMP}"/04506_data4.csv.gz

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path --query "SELECT * FROM test_insert_format_compression ORDER BY id"

rm -f "${CLICKHOUSE_TMP}"/04506_data4.csv "${CLICKHOUSE_TMP}"/04506_data4.csv.gz
rm -rf "${CLICKHOUSE_TMP}"/04506_local_path

# clickhouse-local regression: COMPRESSION works through the input() table function in clickhouse-local
# too, not just through a bare FORMAT clause. clickhouse-local's input() reads via a separate
# LocalConnection::setInputInitializer() path that does not go through ClientBase::sendDataFrom(), so it
# needs (and has) its own handling of the COMPRESSION clause. As with the bare-FORMAT case above, the
# INSERT fed via stdin must be the last statement in its query text, so CREATE and INSERT run in one
# invocation with a persistent --path, and SELECT runs in a second invocation. Uses MergeTree, not
# Memory, since Memory-engine data does not survive across clickhouse-local process restarts even with
# the same --path.
printf '13,M\n14,N\n' > "${CLICKHOUSE_TMP}"/04506_data5.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data5.csv

rm -rf "${CLICKHOUSE_TMP}"/04506_local_path2
mkdir -p "${CLICKHOUSE_TMP}"/04506_local_path2

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path2 --query "
CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_insert_format_compression SELECT * FROM input('id UInt32, text String') FORMAT CSV COMPRESSION 'gzip'
" < "${CLICKHOUSE_TMP}"/04506_data5.csv.gz

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path2 --query "SELECT * FROM test_insert_format_compression ORDER BY id"

rm -f "${CLICKHOUSE_TMP}"/04506_data5.csv "${CLICKHOUSE_TMP}"/04506_data5.csv.gz
rm -rf "${CLICKHOUSE_TMP}"/04506_local_path2

# clickhouse-local regression: COMPRESSION 'auto' through the input() table function must resolve to
# the compression already detected from the real stdin descriptor (threaded from LocalServer into
# LocalConnection via setDefaultInputCompressionMethod), not silently become a no-op the way it would
# if resolved with an empty path hint.
printf '17,Q\n18,R\n' > "${CLICKHOUSE_TMP}"/04506_data7.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data7.csv

rm -rf "${CLICKHOUSE_TMP}"/04506_local_path3
mkdir -p "${CLICKHOUSE_TMP}"/04506_local_path3

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path3 --query "
CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_insert_format_compression SELECT * FROM input('id UInt32, text String') FORMAT CSV COMPRESSION 'auto'
" < "${CLICKHOUSE_TMP}"/04506_data7.csv.gz

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path3 --query "SELECT * FROM test_insert_format_compression ORDER BY id"

rm -f "${CLICKHOUSE_TMP}"/04506_data7.csv "${CLICKHOUSE_TMP}"/04506_data7.csv.gz
rm -rf "${CLICKHOUSE_TMP}"/04506_local_path3

# Negative check: COMPRESSION is client-side-only. Sent directly to the server over HTTP (which never
# decompresses this clause), it must be rejected with a clear error instead of falling through to the
# format parser with still-compressed bytes.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
printf '11,K\n' | gzip | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_insert_format_compression%20FORMAT%20CSV%20COMPRESSION%20'gzip'" --data-binary @- | grep -c -o "Query has COMPRESSION next to FORMAT and was send directly to server"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: same rejection applies on the async_insert path, which reads the insert data via a
# different function (AsynchronousInsertQueue::pushQueryWithInlinedData) that bypasses the synchronous
# insert's format-preparation code entirely, so it needs (and has) its own copy of this guard.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
printf '12,L\n' | gzip | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_insert_format_compression%20FORMAT%20CSV%20COMPRESSION%20'gzip'&async_insert=1&wait_for_async_insert=1" --data-binary @- | grep -c -o "Query has COMPRESSION next to FORMAT and was send directly to server"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: FROM INFILE is likewise client-side-only (the server must never open a path from the
# query text itself), and that ban must hold on the async_insert path too. `executeQuery` sets `tail` on
# every INSERT before the async_insert decision, so `hasInlinedData()` is true even for a bare `FROM
# INFILE` with no other data, and this query would otherwise reach pushQueryWithInlinedData() and open
# the path server-side.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
printf '' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_insert_format_compression%20FROM%20INFILE%20'04506_nonexistent.csv'%20FORMAT%20CSV&async_insert=1&wait_for_async_insert=1" --data-binary @- | grep -c -o "Query has infile and was send directly to server"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

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
