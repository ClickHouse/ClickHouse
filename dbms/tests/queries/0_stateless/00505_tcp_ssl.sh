#!/usr/bin/env bash

# Not default server config needed

tcp_ssl_port=`clickhouse extract-from-config -c /etc/clickhouse-server/config.xml -k tcp_ssl_port 2>/dev/null`
if [ -z ${tcp_ssl_port} ]; then
    # Secure port disabled. Fake result
    echo 1
    echo 2
else
    # Auto port detect
    clickhouse-client --secure -q "SELECT 1";
    clickhouse-client --secure --port=9440 -q "SELECT 2";
fi
