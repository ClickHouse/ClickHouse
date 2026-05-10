#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. Without `allow_experimental_json_ast_dialect`, the `clickhouse_json` dialect
#    must be rejected with `SUPPORT_IS_DISABLED`.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json" --data-binary @- <<< 'SELECT 1' \
    2>&1 | grep -om1 'SUPPORT_IS_DISABLED'

# 2. With the experimental setting on, plain `SET` queries are still accepted
#    while `dialect = 'clickhouse_json'`, so users can switch back to another dialect.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_json_ast_dialect=1&dialect=clickhouse_json" --data-binary @- <<< "SET dialect = 'clickhouse'"
echo 'set ok'

# 3. With the experimental setting on, a valid JSON-encoded query produced by
#    `parseQueryToJSON` is accepted through the `clickhouse_json` dialect.
JSON=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT parseQueryToJSON('SELECT 42 AS answer') FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_json_ast_dialect=1&dialect=clickhouse_json" --data-binary "$JSON"

# 4. The same `SET` escape hatch must work in `clickhouse-client` so an interactive
#    user who issues `SET dialect = 'clickhouse_json'` can still switch back.
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --multiquery -q "SET dialect = 'clickhouse_json'; SET dialect = 'clickhouse'; SELECT 100;"

# 5. The same `SET` escape hatch must work in `clickhouse-local`.
${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --multiquery -q "SET dialect = 'clickhouse_json'; SET dialect = 'clickhouse'; SELECT 200;"
