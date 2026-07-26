#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for _ in {1..10}; do
    # Match the <Trace> marker anywhere: the log prefix omits host_name/query_id when
    # empty, so its column is not fixed and a positional awk '{print $8}' is fragile.
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT * from numbers(1000000);" 2>&1 | grep -q '<Trace>' && echo "OK" || echo "Fail" &
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query="SELECT * from numbers(1000000);" 2>&1 | grep '<Debug>\|<Trace>' &
done

wait
