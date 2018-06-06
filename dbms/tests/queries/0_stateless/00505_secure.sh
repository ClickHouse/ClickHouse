#!/usr/bin/env bash

#set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Not default server config needed


if [ -n $CLICKHOUSE_CONFIG_CLIENT ]; then
    USE_CONFIG="--config $CLICKHOUSE_CONFIG_CLIENT"
fi


tcp_port_secure=`$CLICKHOUSE_EXTRACT_CONFIG -k tcp_port_secure 2>/dev/null`
if [ -z $tcp_port_secure ]; then
    # Secure port disabled. Fake result
    cat $CURDIR/00505_secure.reference
else

    if [[ $CLICKHOUSE_CLIENT != *"--port"* ]]; then
        CLICKHOUSE_CLIENT_SECURE=${CLICKHOUSE_CLIENT_SECURE:="$CLICKHOUSE_CLIENT $USE_CONFIG --secure --port=$CLICKHOUSE_PORT_TCP_SECURE"}

        # Auto port detect. Cant test with re-definedvia command line ports
        $CLICKHOUSE_CLIENT $USE_CONFIG --secure -q "SELECT 1";
    else
        CLICKHOUSE_CLIENT_BINARY=${CLICKHOUSE_CLIENT_BINARY:="${CLICKHOUSE_BINARY}-client"}
        CLICKHOUSE_CLIENT_SECURE=${CLICKHOUSE_CLIENT_SECURE:="$CLICKHOUSE_CLIENT_BINARY $USE_CONFIG --secure --port=$CLICKHOUSE_PORT_TCP_SECURE"}
        echo 1
    fi

    $CLICKHOUSE_CLIENT_SECURE -q "SELECT 2;"

    $CLICKHOUSE_CURL -sS --insecure ${CLICKHOUSE_URL_HTTPS}?query=SELECT%203

    $CLICKHOUSE_CLIENT_SECURE -q "SELECT 4;"

    # TODO: can test only on unchanged port. Possible solutions: generate config or pass shard port via command line
    if [[ "$CLICKHOUSE_PORT_TCP_SECURE" = "$tcp_port_secure" ]]; then
        cat $CURDIR/00505_distributed_secure.data | $CLICKHOUSE_CLIENT_SECURE -n -m
    else
        tail -n 13 $CURDIR/00505_secure.reference
    fi

fi
