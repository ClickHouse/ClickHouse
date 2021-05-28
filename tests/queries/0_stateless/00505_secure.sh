#!/usr/bin/env bash

# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Not default server config needed


if [ -n "$CLICKHOUSE_CONFIG_CLIENT" ]; then
    USE_CONFIG="--config-file $CLICKHOUSE_CONFIG_CLIENT"
fi

CLICKHOUSE_CLIENT_SECURE=${CLICKHOUSE_CLIENT_SECURE:="$CLICKHOUSE_CLIENT_BINARY $USE_CONFIG --secure --port=$CLICKHOUSE_PORT_TCP_SECURE"}
if [[ $CLICKHOUSE_CLIENT != *"--port"* ]]; then
    # Auto port detect. Cant test with re-defined via command line ports
    $CLICKHOUSE_CLIENT_BINARY $USE_CONFIG --secure -q "SELECT 1";
else
    echo 1
fi

$CLICKHOUSE_CLIENT_SECURE -q "SELECT 2;"

#disable test
#$CLICKHOUSE_CURL -sS --insecure ${CLICKHOUSE_URL_HTTPS}?query=SELECT%203
echo 3

$CLICKHOUSE_CLIENT_SECURE -q "SELECT 4;"

# TODO: can test only on unchanged port. Possible solutions: generate config or pass shard port via command line
if [[ "$CLICKHOUSE_PORT_TCP_SECURE" = "$CLICKHOUSE_PORT_TCP_SECURE" ]]; then
    cat "$CURDIR"/00505_distributed_secure.data | $CLICKHOUSE_CLIENT_SECURE -n -m
else
    tail -n 13 "$CURDIR"/00505_secure.reference
fi

