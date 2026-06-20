#!/usr/bin/env bash
# Tags: linux, no-asan, no-tsan, no-msan, no-ubsan

# Verify that asynchronous metrics for HTTP connection pool TCP buffer memory
# (HTTPConnectionPool*TCP{Rcv,Snd}BufBytes_{p50,p75,p90,p95} and TotalBytes)
# are populated when there are active HTTP connection pool connections.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Make several HTTP requests via url() table function to populate the connection pool.
# The url() function uses HTTPConnectionGroupType::STORAGE.
# Keep-alive connections remain tracked in the pool after the query finishes.
for _ in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(2)', LineAsString) FORMAT Null" &
    sleep 0.1
done

# Force an async metrics update while keep-alive connections are in the pool.
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"

# Check that the Storage group TCP buffer metrics exist with non-negative values.
${CLICKHOUSE_CLIENT} -q "
    SELECT metric, value >= 0
    FROM system.asynchronous_metrics
    WHERE metric LIKE 'HTTPConnectionPoolStorageTCP%BufBytes\_%'
       OR metric LIKE 'HTTPConnectionPoolStorageTCP%BufTotalBytes'
    ORDER BY metric
"

wait
