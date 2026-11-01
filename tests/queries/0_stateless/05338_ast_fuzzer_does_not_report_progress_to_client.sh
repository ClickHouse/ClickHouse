#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fuzzed queries run after the query and before its HTTP response is finalized:
# their progress must not reach the response of the query they were derived from.
QUERY='SELECT number FROM numbers(10) FORMAT Null'

${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&ast_fuzzer_runs=10" --data-binary "$QUERY" 2>&1 \
    | grep -F 'X-ClickHouse-Summary:' | grep -o '"read_rows":"[0-9]*"'

${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&ast_fuzzer_runs=10&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" \
    --data-binary "$QUERY" 2>&1 \
    | grep -F 'X-ClickHouse-Progress:' | grep -o '"read_rows":"[0-9]*"' | grep -o '[0-9]\+' | sort -n | tail -1
