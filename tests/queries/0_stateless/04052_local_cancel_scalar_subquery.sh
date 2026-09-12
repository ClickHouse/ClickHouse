#!/usr/bin/env bash
# Tags: no-fasttest
# Test that clickhouse-local can cancel scalar subqueries via SIGINT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

stderr_file="${CLICKHOUSE_TMP}/04052_local_cancel_scalar_subquery_${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"
rm -f "$stderr_file"

# Start a query with an infinite scalar subquery
${CLICKHOUSE_LOCAL} --query="SELECT (SELECT max(number) FROM system.numbers) + 1 SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0" >/dev/null 2>"$stderr_file" &
local_pid=$!

sleep 1

# Send SIGINT to cancel the query
kill -INT $local_pid 2>/dev/null

# Wait for the process to exit — cancellation should be near-instant,
# but give enough margin for slow CI machines.
for _ in {0..60}
do
    if ! kill -0 $local_pid 2>/dev/null; then
        wait $local_pid 2>/dev/null

        if ! grep -q 'Code: 394' "$stderr_file" || ! grep -q '(QUERY_WAS_CANCELLED)' "$stderr_file"
        then
            echo "WRONG CANCELLATION EXCEPTION"
            cat "$stderr_file"
            rm -f "$stderr_file"
            exit 1
        fi

        if grep -q 'Code: 735' "$stderr_file" || grep -q 'QUERY_WAS_CANCELLED_BY_CLIENT' "$stderr_file"
        then
            echo "NATIVE CANCEL EXCEPTION LEAKED INTO LOCAL CANCELLATION"
            cat "$stderr_file"
            rm -f "$stderr_file"
            exit 1
        fi

        rm -f "$stderr_file"
        echo "CANCELLED"
        exit 0
    fi
    sleep 0.5
done

# If still running after 30 seconds, cancellation failed
kill -9 $local_pid 2>/dev/null
wait $local_pid 2>/dev/null
rm -f "$stderr_file"
echo "HUNG"
