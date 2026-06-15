#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: requires transactions (allow_experimental_transactions), not enabled in fast test
# no-ordinary-database: transactions require an Atomic database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107446

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The outer SELECT executes (and streams its result) before the inline AST fuzzer clears
# the implicit transaction, so its output is deterministic. --fail-with-body makes curl
# return non-zero on an HTTP 500, so a release-build LOGICAL_ERROR from the pre-fix commit
# path is caught here, not only a debug-build server abort (detected by the liveness check).
${CLICKHOUSE_CURL} --fail-with-body -sS "${CLICKHOUSE_URL}&implicit_transaction=1&throw_on_unsupported_query_inside_transaction=0&ast_fuzzer_runs=5" \
    --data-binary "SELECT 1 FROM numbers(3)" || exit 1

# The server must still be alive after the fuzzed implicit-transaction query.
${CLICKHOUSE_CLIENT} -q "SELECT 'ok'"
