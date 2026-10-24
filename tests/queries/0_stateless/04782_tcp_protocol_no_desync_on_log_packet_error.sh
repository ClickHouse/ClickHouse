#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires failpoints (libfiu), which are not enabled in the fast test build.
# no-parallel: the failpoints below make writing the log block fail for every query that asks
# for logs, and the test runner passes --send_logs_level to all of them, so leaving this test
# parallel breaks concurrent tests rather than only losing its own coverage. Failpoints cannot
# be scoped to one session: SYSTEM ENABLE FAILPOINT takes only a name (ParserSystemQuery.cpp)
# and FailPointInjection::enableFailPoint keys on it alone (Common/FailPoint.h).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query of case 2 goes through a file: it is far too long to pass as an argument.
QUERY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"

# The failpoints are process-global, so an abnormal exit anywhere below would leave one armed and
# every later query that asks for logs would lose its log packet.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_mid_block" || true
      ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_after_flush" || true
      ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT tcp_handler_throw_memory_limit_in_table_columns" || true
      rm -f "${QUERY_FILE:-}"' EXIT

# Both cases below run a query that fails during analysis, so the log packet is the first block
# written to the connection, and a failpoint makes serializing it throw MEMORY_LIMIT_EXCEEDED
# part-way through. grep -a: a desynchronized response carries raw packet bytes, which makes grep
# treat its input as binary and print nothing, hiding the difference being asserted.

# 1. The packet is still fully buffered, so it is rolled back and the real error is delivered.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT native_writer_throw_memory_limit_mid_block"

${CLICKHOUSE_CLIENT} --send_logs_level=error -q \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}.table_that_does_not_exist" 2>&1 \
    | grep -aoE 'UNKNOWN_TABLE|Unrecognized token|SYNTAX_ERROR' | sort -u

# The failpoint must actually have fired: when it does the log packet is rolled back and never
# reaches the client, so the server-side log line it carries is absent. Without this the case
# above would pass even if the failpoint were renamed or stopped being reached.
${CLICKHOUSE_CLIENT} --send_logs_level=error -q \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}.table_that_does_not_exist" 2>&1 \
    | grep -ac '<Error> executeQuery'

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_mid_block"

# 2. The log block is larger than the output buffer, so part of it has already been sent and the
# rollback is impossible. The connection must be closed rather than left desynchronized: without
# that, the client waits for the rest of the aborted packet until the socket timeout.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT native_writer_throw_memory_limit_after_flush"

python3 -c "print('SELECT * FROM ${CLICKHOUSE_DATABASE}.nonexistent_' + 'x' * 2000000)" > "$QUERY_FILE"

${CLICKHOUSE_CLIENT} --send_logs_level=error --max_query_size=100000000 --receive_timeout=30 \
    --queries-file "$QUERY_FILE" 2>&1 \
    | grep -aoE 'CANNOT_READ_ALL_DATA|NETWORK_ERROR|ATTEMPT_TO_READ_AFTER_EOF|SOCKET_TIMEOUT|Unrecognized token' | sort -u

# 2b. The same packet with compression enabled. The compressed buffer writes through the socket
# buffer, so a compressed chunk that left it is still held there and the whole packet is still
# undoable: the real error must be delivered instead of the connection being closed. Asserting the
# error is not enough on its own, because it is also what a run reaching no failpoint prints, so
# the count of the log line the aborted packet carried follows: rolled back, it never arrives.
${CLICKHOUSE_CLIENT} --compression=1 --send_logs_level=error --max_query_size=100000000 \
    --receive_timeout=30 --queries-file "$QUERY_FILE" 2>&1 \
    | grep -aoE 'UNKNOWN_TABLE|CANNOT_READ_ALL_DATA|ATTEMPT_TO_READ_AFTER_EOF|SOCKET_TIMEOUT|Unrecognized token' | sort -u

${CLICKHOUSE_CLIENT} --compression=1 --send_logs_level=error --max_query_size=100000000 \
    --receive_timeout=30 --queries-file "$QUERY_FILE" 2>&1 \
    | grep -ac '<Error> executeQuery'

rm -f "$QUERY_FILE"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_after_flush"

# The connection is usable afterwards, whether it was preserved or reopened.
${CLICKHOUSE_CLIENT} -q "SELECT 'after', 1"

# 3. The same invariant on the INSERT handshake: TableColumns precedes the schema string, which is
# materialized under the query memory tracker. Without the rollback the client reads the following
# Exception packet as the schema and reports CANNOT_PARSE_INPUT_ASSERTION_FAILED instead of 241.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.insert_handshake"
${CLICKHOUSE_CLIENT} -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.insert_handshake (a UInt64, b UInt64 DEFAULT a + 1) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT tcp_handler_throw_memory_limit_in_table_columns"

# send_logs_level=none: the runner enables server logs by default, and the log packet carrying
# this same error would match the token below on its own, so the assertion would hold even if the
# Exception packet never arrived.
echo '1' | ${CLICKHOUSE_CLIENT} --send_logs_level=none --receive_timeout=30 -q \
    "INSERT INTO ${CLICKHOUSE_DATABASE}.insert_handshake (a) FORMAT CSV" 2>&1 \
    | grep -aoE 'MEMORY_LIMIT_EXCEEDED|CANNOT_PARSE_INPUT_ASSERTION_FAILED|Unrecognized token' | sort -u

# input() takes the other sendTableColumns call site, reached through the query pipeline.
echo '1' | ${CLICKHOUSE_CLIENT} --send_logs_level=none --receive_timeout=30 -q \
    "INSERT INTO ${CLICKHOUSE_DATABASE}.insert_handshake (a) SELECT * FROM input('a UInt64') FORMAT CSV" 2>&1 \
    | grep -aoE 'MEMORY_LIMIT_EXCEEDED|CANNOT_PARSE_INPUT_ASSERTION_FAILED|Unrecognized token' | sort -u

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT tcp_handler_throw_memory_limit_in_table_columns"

${CLICKHOUSE_CLIENT} -q "SELECT 'after insert', count() FROM ${CLICKHOUSE_DATABASE}.insert_handshake"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.insert_handshake"
