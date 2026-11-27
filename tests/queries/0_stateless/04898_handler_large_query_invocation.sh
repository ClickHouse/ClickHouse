#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler whose stored query text is longer than the default `max_query_size` must stay invokable. Creating
# the handler and reloading it parse that text without a size limit, so the invocation path must not apply the
# caller's limit to the server's own text either - otherwise such a handler is creatable and reloadable but
# fails on every request.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_bigquery_${CLICKHOUSE_DATABASE}"
URL="/bigquery_${CLICKHOUSE_DATABASE}"

# Longer than the default `max_query_size` of 262144, so the creating session has to raise it. The queries are
# fed through stdin: a single argument this long exceeds the kernel's per-argument limit.
BIG="$(python3 -c "print('x' * 300000)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created ==="
echo "CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT length('${BIG}')" \
    | ${CLICKHOUSE_CLIENT} --max_query_size 1000000 && echo "created"

echo "=== the default request limit rejects the same query text sent by a client ==="
echo "SELECT length('${BIG}')" | ${CLICKHOUSE_CURL} -sS "${BASE}/" --data-binary @- 2>&1 | grep -o -m1 "Max query size exceeded"

echo "=== invoked over HTTP with the default limit ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
