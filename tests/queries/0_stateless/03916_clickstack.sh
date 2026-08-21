#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickstack UI is accessible and serves correct content
${CLICKHOUSE_CURL} --compressed -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/clickstack" | grep -oF 'ClickStack'

# Test that clickstack serves with gzip encoding
${CLICKHOUSE_CURL} -sS -I "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/clickstack" | grep -oF 'Content-Encoding: gzip'

# Test that 404 is returned for non-existent resources
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/clickstack/nonexistent" | grep -oF 'Not found'
