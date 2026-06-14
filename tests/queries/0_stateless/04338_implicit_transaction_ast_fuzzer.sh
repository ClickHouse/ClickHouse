#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: requires transactions (allow_experimental_transactions), not enabled in fast test
# no-ordinary-database: transactions require an Atomic database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107446
# The server-side AST fuzzer (ast_fuzzer_runs > 0) resets the query context's current
# transaction between fuzz iterations. With implicit_transaction = 1 the end-of-query
# commit then read a now-null transaction and hit chassert(txn) in
# ImplicitTransactionControlExecutor::commit, aborting the server. The fuzzer runs inline
# (before the commit) only on the HTTP query path, so the crash is reproduced over HTTP.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A single read-only query is enough: the fuzzer clears the implicit transaction,
# then the end-of-query commit must not assert. Before the fix this aborts the server.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&implicit_transaction=1&throw_on_unsupported_query_inside_transaction=0&ast_fuzzer_runs=5" \
    --data-binary "SELECT 1 FROM numbers(3)" >/dev/null 2>&1

# The server must still be alive after the fuzzed implicit-transaction query.
${CLICKHOUSE_CLIENT} -q "SELECT 'ok'"
