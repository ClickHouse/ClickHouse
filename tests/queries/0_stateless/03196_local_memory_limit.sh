#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --config-file <(echo "<clickhouse><max_server_memory_usage>100M</max_server_memory_usage></clickhouse>") --query "SELECT number FROM system.numbers GROUP BY number HAVING count() > 1" 2>&1 | grep -o -P 'maximum: [\d\.]+ MiB'
