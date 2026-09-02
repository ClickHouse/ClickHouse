#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&max_memory_usage=1G" -d "SELECT range(65535) FROM system.one ARRAY JOIN range(65536) AS number" | grep -oF 'Code: 241'
