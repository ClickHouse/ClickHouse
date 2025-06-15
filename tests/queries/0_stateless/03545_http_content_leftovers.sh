#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel - spawning bunch of processes with sanitizers can use significant amount of memory

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -v -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/js/uplot.js" 2>&1 | grep -o -P 'Leftovers after chunking|left intact'
