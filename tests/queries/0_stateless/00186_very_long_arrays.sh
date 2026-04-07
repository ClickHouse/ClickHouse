#!/usr/bin/env bash
# Tags: long, no-tsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

(echo 'SELECT number FROM system.numbers WHERE transform(number, ['; seq 1 100000 | tr '\n' ','; echo '0],['; seq 1 100000 | tr '\n' ','; echo '0]) = 10000000 LIMIT 1';) | $CLICKHOUSE_CLIENT --max_query_size=100000000
