#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# no-fasttest: Slow wait and retries

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


i=0 retries=5
# Connecting to wrong address and checking for race condition
# http_max_tries is limited to 2 because with the default 10 retries the execution time might go as high as around 3 minutes (because of exponential back-off).
# because of that we might see wrong 'tests hung' reports depending on how close to the end of tests run this particular test was executed.
# proper fix should be implemented in https://github.com/ClickHouse/ClickHouse/issues/66656
while [[ $i -lt $retries ]]; do
    timeout 5s ${CLICKHOUSE_CLIENT} --max_threads 10 --http_max_tries 2 --query "SELECT * FROM url('http://128.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(1)', TSV, 'x UInt8')" --format Null 2>/dev/null
    ((++i))
done
