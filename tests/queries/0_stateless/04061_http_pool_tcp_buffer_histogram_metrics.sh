#!/usr/bin/env bash
# Tags: linux, no-asan, no-tsan, no-msan, no-ubsan

# Verify that the http_pool_tcp_buf_bytes histogram metric is populated when
# there are active HTTP connection pool connections.

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

# The +Inf bucket of the histogram is the cumulative count of all observations.
# After the metrics scrape above, every live storage-pool socket should have been
# observed exactly once for each of rcv/snd, so the count must be > 0.
${CLICKHOUSE_CLIENT} -q "
    SELECT labels['direction'], value > 0
    FROM system.histogram_metrics
    WHERE metric = 'http_pool_tcp_buf_bytes'
      AND labels['group'] = 'storage'
      AND labels['le'] = '+Inf'
    ORDER BY labels['direction']
"

wait
