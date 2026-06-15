#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: requires transactions (allow_experimental_transactions), not enabled in fast test
# no-ordinary-database: transactions require an Atomic database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107446
# The server-side AST fuzzer (ast_fuzzer_runs > 0) used to reset the *current* query's transaction.
# With implicit_transaction = 1 that rolled the still-running transaction back out of band: the
# end-of-query commit then hit chassert(txn) and aborted the server, and an INSERT silently lost its
# rows. The fuzzer now isolates fuzzed queries from the original transaction, so the query succeeds
# and its transaction commits normally. The fuzzer runs only on the HTTP query path, so the fuzzed
# queries are issued over HTTP. --fail-with-body makes curl fail on a non-2xx response, and the query
# output is asserted against the reference, so an exception (even one that keeps the server alive) is
# caught instead of silently passing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FUZZ="implicit_transaction=1&throw_on_unsupported_query_inside_transaction=0&ast_fuzzer_runs=5"

# 1. A read-only fuzzed query must succeed (not return an exception) and not abort the server.
${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&${FUZZ}" --data-binary "SELECT count() FROM numbers(3)"

# 2. An INSERT under the same settings must succeed and commit its rows (not silently roll them back).
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04338 (a Int64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&${FUZZ}" --data-binary "INSERT INTO t_04338 VALUES (42)"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(a) FROM t_04338"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04338"

# The server must still be alive after the fuzzed implicit-transaction queries.
${CLICKHOUSE_CLIENT} -q "SELECT 'ok'"
