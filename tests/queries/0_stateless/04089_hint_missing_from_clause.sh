#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Query without FROM clause should hint that FROM is missing
$CLICKHOUSE_CLIENT -q "SELECT x" 2>&1 | grep -o 'Did you forget to add it?'

# Query with multiple columns and no FROM should also hint
$CLICKHOUSE_CLIENT -q "SELECT check_name, count(), sum(dur) AS t WHERE name = 'test' GROUP BY check_name ORDER BY t DESC" 2>&1 | grep -o 'Did you forget to add it?'

# Query with FROM clause should NOT produce the hint
$CLICKHOUSE_CLIENT -q "SELECT nonexistent_column FROM system.one" 2>&1 | grep -c 'Did you forget to add it?' | sed 's/0/no hint for query with FROM/'
