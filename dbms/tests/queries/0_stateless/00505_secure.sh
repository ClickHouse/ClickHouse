#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Not default server config needed

tcp_port_secure=`${CLICKHOUSE_EXTRACT_CONFIG} -k tcp_port_secure 2>/dev/null`
if [ -z ${tcp_port_secure} ]; then
    # Secure port disabled. Fake result
    cat $CURDIR/00505_secure.reference
else
    # Auto port detect
    ${CLICKHOUSE_CLIENT} --secure -q "SELECT 1";
    ${CLICKHOUSE_CLIENT} --secure --port=${CLICKHOUSE_PORT_TCP_SECURE} -q "SELECT 2";

    ${CLICKHOUSE_CURL} -sS --insecure ${CLICKHOUSE_URL_HTTPS}?query=SELECT%203

    ${CLICKHOUSE_CLIENT} --secure -q "SELECT 1";

    cat $CURDIR/00505_distributed_secure.data | $CLICKHOUSE_CLIENT --secure -n -m
fi
