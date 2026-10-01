#!/usr/bin/env bash
# The `SET` fast path of `parseKQLQuery` must reject leftover statements the same way the main
# path does: a server-side query is a single statement, and `SET dialect = 'clickhouse'; SELECT 1`
# used to run the `SET` and silently drop the `SELECT`. The gate-off escape hatch of
# `tryParseKQLSetStatement` fires only when the whole query is that one `SET` statement.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url_gate_on="${CLICKHOUSE_URL}&dialect=kusto&allow_experimental_kusto_dialect=1"
url_gate_off="${CLICKHOUSE_URL}&dialect=kusto&allow_experimental_kusto_dialect=0"

echo '-- a single SET works, with and without the gate --'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" -d "set max_threads = 1" && echo OK
${CLICKHOUSE_CURL} -sS "${url_gate_off}" -d "set max_threads = 1" && echo OK

echo '-- a statement after the SET is rejected, not silently dropped --'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" -d "set max_threads = 1; print 1" | grep -c 'Multi-statements are not allowed'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" -d "set dialect = 'clickhouse'; select 1" | grep -c 'Multi-statements are not allowed'

echo '-- with the gate off the escape hatch does not fire for a multi-statement --'
${CLICKHOUSE_CURL} -sS "${url_gate_off}" -d "set dialect = 'clickhouse'; select 1" | grep -c 'SUPPORT_IS_DISABLED'
