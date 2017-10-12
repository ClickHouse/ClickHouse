#!/usr/bin/env bash

# Not default server config needed

. ./99999_sh_lib.sh

tcp_ssl_port=`${CLICKHOUSE_EXTRACT_CONFIG} -k tcp_ssl_port 2>/dev/null`
if [ -z ${tcp_ssl_port} ]; then
    # Secure port disabled. Fake result
    cat ./00505_tcp_ssl.reference
else
    # Auto port detect
    ${CLICKHOUSE_CLIENT} --ssl -q "SELECT 1";
    ${CLICKHOUSE_CLIENT} --ssl --port=9440 -q "SELECT 2";
fi
