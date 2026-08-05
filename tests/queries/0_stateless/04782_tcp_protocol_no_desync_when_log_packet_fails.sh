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

# Both cases below run a query that fails during analysis, so the log packet is the first block
# written to the connection, and a failpoint makes serializing it throw MEMORY_LIMIT_EXCEEDED
# part-way through. grep -a: a desynchronized response carries raw packet bytes, which makes grep
# treat its input as binary and print nothing, hiding the difference being asserted.

# 1. The packet is still fully buffered, so it is rolled back and the real error is delivered.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT native_writer_throw_memory_limit_mid_block"

${CLICKHOUSE_CLIENT} --send_logs_level=error -q \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}.table_that_does_not_exist" 2>&1 \
    | grep -aoE 'UNKNOWN_TABLE|Unrecognized token|SYNTAX_ERROR' | sort -u

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_mid_block"

# 2. The log block is larger than the output buffer, so part of it has already been sent and the
# rollback is impossible. The connection must be closed rather than left desynchronized: without
# that, the client waits for the rest of the aborted packet until the socket timeout.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT native_writer_throw_memory_limit_after_flush"

# The query goes through a file: it is far too long to pass as a command line argument.
QUERY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
python3 -c "print('SELECT * FROM ${CLICKHOUSE_DATABASE}.nonexistent_' + 'x' * 2000000)" > "$QUERY_FILE"

${CLICKHOUSE_CLIENT} --send_logs_level=error --max_query_size=100000000 --receive_timeout=30 \
    --queries-file "$QUERY_FILE" 2>&1 \
    | grep -aoE 'CANNOT_READ_ALL_DATA|NETWORK_ERROR|ATTEMPT_TO_READ_AFTER_EOF|SOCKET_TIMEOUT|Unrecognized token' | sort -u

rm -f "$QUERY_FILE"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT native_writer_throw_memory_limit_after_flush"

# The connection is usable afterwards, whether it was preserved or reopened.
${CLICKHOUSE_CLIENT} -q "SELECT 'after', 1"
