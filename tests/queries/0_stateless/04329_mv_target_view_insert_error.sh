#!/usr/bin/env bash
# Tags: no-ordinary-database

set -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_04329; DROP VIEW IF EXISTS vw_04329; DROP TABLE IF EXISTS mv_04329;"

# The source table is pinned to the local disk: StorageMergeTree only supports transactions on
# disks that can append, so on an object storage policy the transactional insert below would fail
# on the source table instead of reaching the check for the view target.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE src_04329 (x Int64) ENGINE=MergeTree ORDER BY x SETTINGS storage_policy = 'default';"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW vw_04329 AS SELECT x FROM src_04329 WHERE x > 10;"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv_04329 TO vw_04329 AS SELECT x FROM src_04329;"

# Print the whole exception message of a failing query on one line, so that an unexpected
# prefix or suffix also changes the output. Prints NO_ERROR if the query unexpectedly succeeds.
run_and_report() {
    local out
    out=$(${CLICKHOUSE_CLIENT} "${@}" 2>&1 >/dev/null | tr '\n' ' ')
    # Keep only the innermost exception text, without the server address or the echoed query.
    out=${out##*DB::Exception: }
    out=$(echo "$out" \
        | sed -E 's/ ?\(query: .*//' \
        | sed -E "s/${CLICKHOUSE_DATABASE}\./{db}./g" \
        | sed -E 's/ \([0-9a-f-]{36}\)//g' \
        | sed -E 's/[[:space:]]+$//')
    if [ -z "$out" ]; then
        echo "NO_ERROR"
    else
        echo "$out"
    fi
}

# Inserting into the source pushes to mv_04329, whose target vw_04329 is a View and cannot be used
# as a materialized view target (even an insertable view would forward the push back into
# src_04329 and recurse forever). The error must name both the materialized view and its target table.
# async_insert is pinned off because the async path appends its own "While executing WaitForAsyncInsert".
run_and_report --async_insert=0 --query "INSERT INTO src_04329 VALUES (100);"

# The same insert inside a transaction fails earlier, in the transaction support check,
# and must name the materialized view as well.
run_and_report --async_insert=0 --implicit_transaction=1 --query "INSERT INTO src_04329 VALUES (101);"

# A direct INSERT into the materialized view goes through the same target, so it is named too.
run_and_report --async_insert=0 --query "INSERT INTO mv_04329 VALUES (102);"

# Inside a transaction the same insert is rejected earlier, by InterpreterInsertQuery, on the
# materialized view itself rather than on its target, so it names only the view. That path is
# outside this change; the line below pins the current message so a later fix has to update it.
run_and_report --async_insert=0 --implicit_transaction=1 --query "INSERT INTO mv_04329 VALUES (103);"

# A direct INSERT into the View is forwarded to src_04329, whose insert pushes to mv_04329 and hits
# the same rejection of a View as a materialized view target, with the context appended.
run_and_report --async_insert=0 --query "INSERT INTO vw_04329 VALUES (100);"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_04329; DROP VIEW IF EXISTS vw_04329; DROP TABLE IF EXISTS mv_04329;"
