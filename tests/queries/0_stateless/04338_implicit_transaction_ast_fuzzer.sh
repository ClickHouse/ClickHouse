#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: requires transactions (allow_experimental_transactions), not enabled in fast test
# no-ordinary-database: transactions require an Atomic database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107446
# The server-side AST fuzzer (ast_fuzzer_runs > 0) used to reset the current query's transaction.
# With implicit_transaction = 1 that rolled the still-running transaction back out of band, so the
# end-of-query commit hit chassert(txn) and aborted the server. The fuzzer now isolates fuzzed
# queries from the original transaction, so the query succeeds and the server stays up. The fuzzer
# runs only on the HTTP query path, so the query is issued over HTTP. --fail-with-body makes curl
# fail on a non-2xx response, and the output is asserted against the reference, so a returned
# exception (even one that keeps the server alive) is caught instead of silently passing.
#
# A read-only query is enough and intentionally used here: it is fuzzed by default
# (ast_fuzzer_any_query defaults to false), and fuzzing non-read-only queries would let fuzzed DDL
# reach the replicated-DDL path, which has a separate, unrelated defect.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# implicit_transaction + AST fuzzer must not abort the server and must return the query result.
${CLICKHOUSE_CURL} -sS --fail-with-body \
    "${CLICKHOUSE_URL}&implicit_transaction=1&throw_on_unsupported_query_inside_transaction=0&ast_fuzzer_runs=5" \
    --data-binary "SELECT count() FROM numbers(3)"

# The server must still be alive after the fuzzed implicit-transaction query.
${CLICKHOUSE_CLIENT} -q "SELECT 'ok'"
