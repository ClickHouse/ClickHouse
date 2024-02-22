#!/usr/bin/env bash

# Retries are necessary because the DNS cache may be flushed before second statement is executed
i=0 retries=3
while [[ $i -lt $retries ]]; do
    ${CLICKHOUSE_CURL} -sS --fail --data "SELECT * FROM url('http://localhost:8123/ping', CSV, 'auto', headers())" "${CLICKHOUSE_URL}" | grep -oP -q 'Ok.' && \
    RECORDS=$(${CLICKHOUSE_CURL} -sS --fail --data "SELECT hostname, ip_address, ip_family, (isNotNull(cached_at) AND cached_at > '1970-01-01 00:00:00') FROM system.dns_cache WHERE hostname = 'localhost' and ip_family = 'IPv4';" "${CLICKHOUSE_URL}")

    if [ "${RECORDS}" != "" ]; then
       echo -n "${RECORDS}"
       exit 0
    fi

    ((++i))
    sleep 0.2
done

echo "All tries to fetch entries for localhost failed, no rows returned.
Probably the DNS cache is disabled or the ClickHouse instance not responds to ping."
exit 1