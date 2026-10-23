#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_read_with_cancel"

$CLICKHOUSE_CLIENT --max_rows_to_read 0 --query_id="$QUERY_ID" --query="SELECT sum(number * 0) FROM numbers(10000000000) SETTINGS partial_result_on_first_cancel=true;" &
pid=$!

for _ in {0..60}
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'" | grep -F '1' && break
    sleep 0.5
done

kill -SIGINT $pid
wait $pid
