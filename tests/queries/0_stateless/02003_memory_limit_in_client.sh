#!/usr/bin/bash -f

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --max_memory_usage_in_client=1 -n -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client=0 -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"
