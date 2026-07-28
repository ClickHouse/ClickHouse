#!/usr/bin/env bash

# Verifies dynamic URL resolution for the embedded ClickStack UI

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

${CLICKHOUSE_CURL} --compressed -sS "${BASE}/clickstack/trace/some-trace-id" \
    | grep -oF 'Redirecting to search' | head -n 1

${CLICKHOUSE_CURL} --compressed -sS "${BASE}/clickstack/search/%5BsavedSearchId%5D.html" \
    | grep -oF 'ClickStack' | head -n 1

${CLICKHOUSE_CURL} --compressed -sS "${BASE}/clickstack/dashboards/list" \
    | grep -oF '"page":"/dashboards/list"' | head -n 1

${CLICKHOUSE_CURL} -sS "${BASE}/clickstack/no-such-page" \
    | grep -oF 'Not found'

# Malformed percent-encoding must produce a deterministic 400
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}/clickstack/%"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}/clickstack/%zz"
