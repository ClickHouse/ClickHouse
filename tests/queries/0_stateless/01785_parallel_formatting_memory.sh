#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --compression 0 --max_memory_usage 1G --query "SELECT range(65535) FROM system.one ARRAY JOIN range(65536) AS number" 2>&1 | grep -oF 'Code: 241' | head -n1
$CLICKHOUSE_CLIENT --compression 1 --max_memory_usage 1G --query "SELECT range(65535) FROM system.one ARRAY JOIN range(65536) AS number" 2>&1 | grep -oF 'Code: 241' | head -n1
