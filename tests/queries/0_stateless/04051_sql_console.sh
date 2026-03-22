#!/usr/bin/env bash
# Tags: no-fasttest

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then
    echo "gzip not found" 1>&2
    exit 1
fi

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

${CLICKHOUSE_CURL} "${BASE_URL}/sql-console" | gzip -d | grep -o 'ClickHouse SQL Console' | head -n1
${CLICKHOUSE_CURL} -I "${BASE_URL}/sql-console" | grep -i '^Content-Encoding:' | tr -d '\r'
${CLICKHOUSE_CURL} "${BASE_URL}/sql-console/env.js" | gzip -d | grep -o 'window\._env'
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/sql-console/monacoeditorwork/editor.worker.bundle.js"
${CLICKHOUSE_CURL} "${BASE_URL}/sql-console/console/newQuery" | gzip -d | grep -o 'ClickHouse SQL Console' | head -n1
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/sql-console/sql-console/monacoeditorwork/editor.worker.bundle.js"
${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}\n' "${BASE_URL}/sql-console/nonexistent.js"
