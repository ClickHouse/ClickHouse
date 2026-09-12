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
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' FORMAT CSV" < "${CLICKHOUSE_TMP}"/04506_data.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data.csv "${CLICKHOUSE_TMP}"/04506_data.csv.gz

# Parser-only check: COMPRESSION next to a bare FORMAT (no FROM INFILE) is accepted and round-trips through formatting.
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression COMPRESSION ''gzip'' FORMAT CSV')"

# Parser-only check: COMPRESSION also round-trips for INSERT ... SELECT ... FROM input('CSV') FORMAT CSV (the input() branch).
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression COMPRESSION ''gzip'' SELECT * FROM input(''id UInt32, text String'') FORMAT CSV')"

# Parser-only check: SETTINGS combined with COMPRESSION must be accepted only in SETTINGS-then-COMPRESSION
# order (matching formatImpl's print order), and the formatted output must itself re-parse -- otherwise
# EXPLAIN AST / query logging / formatQuerySingleLine would produce text the parser rejects.
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression SETTINGS max_threads = 1 COMPRESSION ''gzip'' FORMAT CSV')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('INSERT INTO test_insert_format_compression SETTINGS max_threads = 1 COMPRESSION ''gzip'' SELECT * FROM input(''id UInt32, text String'') FORMAT CSV')"

# Functional check: COMPRESSION works the same way through the input() table function as through a bare FORMAT clause.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"

printf '3,C\n4,D\n' > "${CLICKHOUSE_TMP}"/04506_data2.csv
gzip -k "${CLICKHOUSE_TMP}"/04506_data2.csv

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' SELECT * FROM input('id UInt32, text String') FORMAT CSV" < "${CLICKHOUSE_TMP}"/04506_data2.csv.gz

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data2.csv "${CLICKHOUSE_TMP}"/04506_data2.csv.gz

# Non-gzip compression method: COMPRESSION works with 'zstd' too, not just 'gzip'.
printf '7,G\n8,H\n' > "${CLICKHOUSE_TMP}"/04506_data3.csv
zstd -q -k -f "${CLICKHOUSE_TMP}"/04506_data3.csv -o "${CLICKHOUSE_TMP}"/04506_data3.csv.zst

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'zstd' FORMAT CSV" < "${CLICKHOUSE_TMP}"/04506_data3.csv.zst
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data3.csv "${CLICKHOUSE_TMP}"/04506_data3.csv.zst

# COMPRESSION 'auto' on the bare-FORMAT stdin path must reuse the compression already
# auto-detected from the redirected stdin file descriptor's name, not silently become a no-op.
printf '15,O\n16,P\n' > "${CLICKHOUSE_TMP}"/04506_data6.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data6.csv

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'auto' FORMAT CSV" < "${CLICKHOUSE_TMP}"/04506_data6.csv.gz
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
INSERT INTO test_insert_format_compression COMPRESSION 'gzip' FORMAT CSV
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
INSERT INTO test_insert_format_compression COMPRESSION 'gzip' SELECT * FROM input('id UInt32, text String') FORMAT CSV
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
INSERT INTO test_insert_format_compression COMPRESSION 'auto' SELECT * FROM input('id UInt32, text String') FORMAT CSV
" < "${CLICKHOUSE_TMP}"/04506_data7.csv.gz

${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP}"/04506_local_path3 --query "SELECT * FROM test_insert_format_compression ORDER BY id"

rm -f "${CLICKHOUSE_TMP}"/04506_data7.csv "${CLICKHOUSE_TMP}"/04506_data7.csv.gz
rm -rf "${CLICKHOUSE_TMP}"/04506_local_path3

# Negative check: COMPRESSION is client-side-only. Sent directly to the server over HTTP (which never
# decompresses this clause), it must be rejected with a clear error instead of falling through to the
# format parser with still-compressed bytes.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
printf '11,K\n' | gzip | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_insert_format_compression%20COMPRESSION%20'gzip'%20FORMAT%20CSV" --data-binary @- | grep -c -o "Query has COMPRESSION next to FORMAT and was send directly to server"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: same rejection applies on the async_insert path, which reads the insert data via a
# different function (AsynchronousInsertQueue::pushQueryWithInlinedData) that bypasses the synchronous
# insert's format-preparation code entirely, so it needs (and has) its own copy of this guard.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
printf '12,L\n' | gzip | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20test_insert_format_compression%20COMPRESSION%20'gzip'%20FORMAT%20CSV&async_insert=1&wait_for_async_insert=1" --data-binary @- | grep -c -o "Query has COMPRESSION next to FORMAT and was send directly to server"
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
echo '5,E' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'bogus' FORMAT CSV" 2>&1 | grep -c -o "Unknown compression method"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: COMPRESSION next to a bare FORMAT is rejected for data embedded inline in the query
# text (as opposed to piped via stdin), since inline data shares one buffer with any following query
# in a multiquery script and compressed streams have no unambiguous end marker for that boundary.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' FORMAT CSV
6,F
" 2>&1 | grep -c -o "only supported for data supplied via stdin"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: same rejection applies to the mirrored input() branch -- COMPRESSION next to
# SELECT ... FROM input() ... FORMAT is rejected the same way for inline query-text data, since
# both carriers share one ClientBase guard (insert->data && insert->compression && !insert->infile).
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' SELECT * FROM input('id UInt32, text String') FORMAT CSV
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
INSERT INTO test_insert_format_compression COMPRESSION 'gzip' FORMAT CSV
6,F
;
SELECT 'still alive';
" 2>&1 | grep -c -x "still alive" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_format_compression"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Positive check: COMPRESSION 'none' next to a bare FORMAT is NOT actually compressed, so it does not
# have the ambiguous-boundary problem above and inline data is allowed with it.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'none' FORMAT CSV
19,S
"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Regression: COMPRESSION 'auto' must resolve against the actual source of the data being
# decompressed, not blindly reuse whatever was auto-detected from a redirected stdin descriptor.
# Here the INSERT data is embedded inline in the query text (plain, uncompressed) with no stdin
# involved at all. 'auto' on the inline chunk must resolve to no compression (there is nothing to
# sniff from query-embedded bytes), or it would try to gunzip plain CSV text and fail.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'auto' FORMAT CSV
22,V
23,W
"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: combining inline query-text data with stdin/infile data is unsupported
# independently of COMPRESSION -- the "Processing inline insert data with both inlined and
# external data" guard in ClientBase rejects it outright whenever inline-insert-data mode is
# active (send_table_structure_on_insert_with_inline_data = 0), regardless of whether the
# inline chunk or the stdin chunk is compressed.
printf '24,X\n' > "${CLICKHOUSE_TMP}"/04506_data9.csv
gzip -k -f "${CLICKHOUSE_TMP}"/04506_data9.csv

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_insert_format_compression COMPRESSION 'auto' FORMAT CSV
22,V
23,W
" < "${CLICKHOUSE_TMP}"/04506_data9.csv.gz 2>&1 | grep -c -o "Processing inline insert data with both inlined and external data"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

rm -f "${CLICKHOUSE_TMP}"/04506_data9.csv "${CLICKHOUSE_TMP}"/04506_data9.csv.gz

# Positive check: since COMPRESSION is now parsed before VALUES/FORMAT/SELECT are recognized (not
# after FORMAT, next to the data), a LineAsString data row that is literally the word `COMPRESSION`,
# or even `COMPRESSION 'gzip'`, can never be mistaken for the clause -- the data zone hasn't opened
# yet at the position where COMPRESSION is looked for. Covers both the bare-FORMAT branch and the
# mirrored input() branch.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (line String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression FORMAT LineAsString
COMPRESSION
COMPRESSION 'gzip'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY line"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (line String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression SELECT * FROM input('line String') FORMAT LineAsString
COMPRESSION
COMPRESSION 'gzip'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_insert_format_compression ORDER BY line"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"

# Negative check: COMPRESSION is only meaningful next to a real data stream (bare FORMAT, input(),
# or FROM INFILE). Next to VALUES, or a plain SELECT with no input(), there is nothing to decompress,
# so the parser rejects it outright instead of silently ignoring it.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_format_compression (id UInt32, text String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' VALUES (1, 'A')" 2>&1 | grep -c -o "COMPRESSION clause is only supported"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_format_compression COMPRESSION 'gzip' SELECT 1, 'A'" 2>&1 | grep -c -o "COMPRESSION clause is only supported"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_insert_format_compression"
