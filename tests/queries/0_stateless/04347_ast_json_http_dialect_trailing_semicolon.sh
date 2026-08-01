#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The server-side single-statement `clickhouse_json` path (executeQuery) must accept a trailing `;`
# delimiter, the same way the SQL path and the JSON multiquery scanner do. The HTTP interface sends the
# raw query string straight to `executeQuery`, so it is the path that exercises this directly.

JSON=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('SELECT 1') FORMAT TSVRaw")
URL="${CLICKHOUSE_URL}&allow_experimental_json_ast_dialect=1&dialect=clickhouse_json"

# 1. A lone JSON AST object executes.
${CLICKHOUSE_CURL} -sS "$URL" --data-binary "$JSON"

# 2. A trailing `;` delimiter (with surrounding whitespace) is consumed, not rejected as malformed JSON.
${CLICKHOUSE_CURL} -sS "$URL" --data-binary "$JSON;"
${CLICKHOUSE_CURL} -sS "$URL" --data-binary "$JSON ; "

# 3. Trailing non-delimiter text after the object is still rejected as excess input, and the object is
#    not executed (no `1` is printed) — assert both.
OUT=$(${CLICKHOUSE_CURL} -sS "$URL" --data-binary "$JSON garbage" 2>&1)
echo "$OUT" | grep -qm1 'BAD_ARGUMENTS' && echo 'error_reported' || echo 'NO_ERROR'
echo "$OUT" | grep -qxF '1' && echo 'PREFIX_EXECUTED' || echo 'prefix_skipped'
