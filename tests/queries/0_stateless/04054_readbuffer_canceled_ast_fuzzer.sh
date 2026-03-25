#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for ReadBuffer canceled by AST fuzzer internal queries.
#
# When a query with ast_fuzzer_runs > 0 completes and the AST fuzzer runs
# fuzzed internal queries, the JoinOrderOptimizer's checkLimits() invokes
# the interactive cancel callback from the outer query context, which calls
# receivePacketsExpectCancel() and reads from the TCP input buffer.
# If the client disconnects at that moment, the input buffer gets canceled.
# Previously, the TCPHandler loop would then call in->eof() on the canceled
# buffer, hitting a chassert in debug builds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.t1 (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.t2 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t1 SELECT number FROM numbers(100);
    INSERT INTO ${CLICKHOUSE_DATABASE}.t2 SELECT number FROM numbers(100);
"

# Run a join query with the AST fuzzer enabled and kill the client mid-execution.
# The fuzzer runs after the original query completes (in onFinish callback), so
# the client needs to disconnect while the fuzzer is still running.
# Use a high ast_fuzzer_runs and low interactive_delay to maximize the chance
# of the cancel callback detecting the disconnection.
# Suppress "Killed" job notifications that bash prints when SIGKILL'd background
# processes are reaped. These messages come from the shell itself, not the child
# processes, so redirections on the child have no effect. Wrapping the loop and
# its wait in a subshell with 2>/dev/null ensures the shell that owns and reaps
# the background jobs has its stderr suppressed, even if a job dies before wait
# is reached and the notification fires asynchronously via SIGCHLD.
(
    for _ in $(seq 1 5); do
        # Sleep a random amount of time between 50ms and 300ms
        SLEEP_TIME="0.$((RANDOM % 251 + 50))"
        (timeout --signal=KILL ${SLEEP_TIME} ${CLICKHOUSE_CLIENT} \
            --ast_fuzzer_runs=500 \
            --interactive_delay=1 \
            -q "SELECT t1.id, t2.id FROM ${CLICKHOUSE_DATABASE}.t1 AS t1 JOIN ${CLICKHOUSE_DATABASE}.t2 AS t2 ON t1.id = t2.id ORDER BY t1.id FORMAT Null" \
        ) > /dev/null 2>&1 &
    done
    wait
) 2>/dev/null

# Verify the server is still alive after the abrupt disconnections.
${CLICKHOUSE_CLIENT} -q "SELECT 'OK'"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t1;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t2;
"
