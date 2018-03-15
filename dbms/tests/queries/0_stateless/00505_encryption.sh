#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Not default server config needed

tcp_ssl_port=`${CLICKHOUSE_EXTRACT_CONFIG} -k tcp_ssl_port 2>/dev/null`
if [ -z ${tcp_ssl_port} ]; then
    # Secure port disabled. Fake result
    cat $CURDIR/00505_encryption.reference
else
    # Auto port detect
    ${CLICKHOUSE_CLIENT} --ssl -q "SELECT 1";
    ${CLICKHOUSE_CLIENT} --ssl --port=${CLICKHOUSE_PORT_TCP_SSL} -q "SELECT 2";

    ${CLICKHOUSE_CURL} -sS --insecure ${CLICKHOUSE_URL_HTTPS}?query=SELECT%203

    ${CLICKHOUSE_CLIENT} --ssl -q "SELECT 1";

    cat $CURDIR/00505_distributed_secure.data | $CLICKHOUSE_CLIENT --ssl -n -m
fi
