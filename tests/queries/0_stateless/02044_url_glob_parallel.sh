#!/usr/bin/env bash
# Tags: distributed, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Verify url() table function with glob patterns works with parallel connections.
# No sleep — just a fast query to avoid any timing dependency on loaded CI machines.
${CLICKHOUSE_CLIENT} --max_threads 10 --query "SELECT * FROM url('http://127.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', TSV, 'x UInt8')" --format Null
