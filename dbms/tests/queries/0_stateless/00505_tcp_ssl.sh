#!/usr/bin/env bash

# Not default server config needed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

tcp_ssl_port=`${CLICKHOUSE_EXTRACT_CONFIG} -k tcp_ssl_port 2>/dev/null`
if [ -z ${tcp_ssl_port} ]; then
    # Secure port disabled. Fake result
    cat $CURDIR/00505_tcp_ssl.reference
else
    # Auto port detect
    ${CLICKHOUSE_CLIENT} --ssl -q "SELECT 1";
    ${CLICKHOUSE_CLIENT} --ssl --port=9440 -q "SELECT 2";
fi
