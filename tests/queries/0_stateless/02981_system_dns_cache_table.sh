#!/usr/bin/env bash

# Retries are necessary because the DNS cache may be flushed before second statment is executed
i=0 retries=3
while [[ $i -lt $retries ]]; do
    ${CLICKHOUSE_CURL} -sS --data "SELECT * FROM url('http://localhost:8123/ping', CSV, 'auto', headers())" "${CLICKHOUSE_URL}" | grep -Eq "Ok."
    ${CLICKHOUSE_CURL} -sS --data "SELECT hostname, ip_address, ip_family, (isNotNull(cached_at) AND cached_at > '1970-01-01 00:00:00') FROM system.dns_cache WHERE hostname = 'localhost';" "${CLICKHOUSE_URL}" | \
    grep -Eq "localhost	::1	IPv6	1
              localhost	127.0.0.1	IPv4	1" && exit 0
    ((++i))
    sleep 0.2
done

exit 1