#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 10 --query "SELECT hostName() FROM remote('128.1.2.3', default.tmp)" 2>&1 | grep -o -P 'connect timed out|Network is unreachable' | wc -l
