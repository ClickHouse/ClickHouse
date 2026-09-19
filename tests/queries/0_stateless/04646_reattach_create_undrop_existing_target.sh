#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself (via reattach_tables_before_query_execution)
# no-replicated-database: DatabaseReplicated does not support the non-permanent DETACH the hook issues

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for the reattach-tables hook (`reattach_tables_before_query_execution`) on statements whose
# interpreter never touches an existing table of the target name: plain `CREATE TABLE dst`,
# `CREATE TABLE IF NOT EXISTS dst`, `ATTACH TABLE dst`, and `UNDROP TABLE dst` either fail with
# `TABLE_ALREADY_EXISTS` or turn into a no-op when `dst` already exists, so the hook must NOT
# `DETACH`/`ATTACH` the existing `dst` for them. The replacing forms — `CREATE OR REPLACE`/`REPLACE` —
# are out of scope as well: they validate the new definition before the replacement path reaches `dst`,
# and whether that validation passes cannot be predicted by the hook, so their destination is never
# eligible either.

MY_CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

function run_with_reattach()
{
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
        --reattach_tables_before_query_execution=1 \
        --query "$1" 2>&1)
    REATTACH_STATUS=$?
}

# The statement must fail with the expected error, and the existing target must not be detached.
function check_fails_without_detach()
{
    run_with_reattach "$1"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "$3"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (existing target was detached for a failing statement)"
    else
        echo "OK"
    fi
}

# The statement must succeed as a no-op, and the existing target must not be detached.
function check_noop_without_detach()
{
    run_with_reattach "$1"
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (existing target was detached for a no-op statement)"
    else
        echo "OK"
    fi
}

# The statement must succeed, and the existing target must not be detached.
function check_succeeds_without_detach()
{
    run_with_reattach "$1"
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (existing target was detached for a replacing statement)"
    else
        echo "OK"
    fi
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_dst VALUES (1)"

check_fails_without_detach "CREATE TABLE t_reattach_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_dst" "TABLE_ALREADY_EXISTS"
check_noop_without_detach "CREATE TABLE IF NOT EXISTS t_reattach_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_dst"
check_fails_without_detach "ATTACH TABLE t_reattach_dst" "t_reattach_dst" "TABLE_ALREADY_EXISTS"
check_fails_without_detach "UNDROP TABLE t_reattach_dst" "t_reattach_dst" "TABLE_ALREADY_EXISTS"

# The data of the original table must be intact after the failing/no-op statements above.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_dst"

# The replacing forms are out of scope: their destination is left alone even when the statement succeeds.
check_succeeds_without_detach "CREATE OR REPLACE TABLE t_reattach_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_dst"
check_succeeds_without_detach "REPLACE TABLE t_reattach_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dst"
