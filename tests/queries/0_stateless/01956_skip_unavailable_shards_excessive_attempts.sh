#!/usr/bin/env bash
# Tags: shard

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--connections_with_failover_max_tries=1"
    "--skip_unavailable_shards=1"
)
$CLICKHOUSE_CLIENT --query "select * from remote('255.255.255.255', system.one)" "${opts[@]}" 2>&1 | grep -o 'Connection failed at try.*,'
