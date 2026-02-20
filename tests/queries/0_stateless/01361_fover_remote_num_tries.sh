#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 10 --connect_timeout_with_failover_ms 1 --query "SELECT hostName() FROM remote('128.1.2.3', default.tmp)" 2>&1 | grep -o -P 'Timeout exceeded while connecting to socket|Network is unreachable|Timeout: connect timed out' | wc -l
