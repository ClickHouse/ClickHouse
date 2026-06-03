#!/usr/bin/env bash

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then
    echo "gzip not found" 1>&2
    exit 1
fi

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

${CLICKHOUSE_CURL} "${BASE_URL}/ui" | gzip -d | grep -o 'ClickHouse SQL Console' | head -n1
${CLICKHOUSE_CURL} -I "${BASE_URL}/ui" | grep -i '^Content-Encoding:' | tr -d '\r'
${CLICKHOUSE_CURL} "${BASE_URL}/ui/env.js" | gzip -d | grep -o 'window\._env'
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/ui/monacoeditorwork/editor.worker.bundle.js"
${CLICKHOUSE_CURL} "${BASE_URL}/ui/console/newQuery" | gzip -d | grep -o 'ClickHouse SQL Console' | head -n1
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/ui/ui/monacoeditorwork/editor.worker.bundle.js"
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/ui/nonexistent.js"
# A sibling route like "/uix" must not be captured by the "/ui" handler (route-boundary check).
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/uix"
