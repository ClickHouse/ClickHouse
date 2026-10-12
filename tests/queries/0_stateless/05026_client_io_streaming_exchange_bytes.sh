#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the streaming exchange of the stateless worker configuration.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The gather of a distributed plan reaches the initiator through a streaming exchange socket, counted as
# StreamingExchangeReceiveBytes. It must reach the client's ProfileEvents stream, which feeds the live IO rate.

# Prints the thread-group total of one counter as the client received it in the ProfileEvents stream.
function client_total()
{
    grep -o "\[ 0 \] $1: [0-9]*" | tail -n 1 | awk '{ print $NF + 0 }'
}

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05026_io_exchange (k String, v UInt64) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05026_io_exchange SELECT concat('key ', toString(number % 1000)), number FROM numbers(100000)"

receive_bytes=$(${CLICKHOUSE_CLIENT} --print-profile-events --profile-events-delay-ms=-1 \
    -q "SELECT count() FROM (SELECT k, count() FROM t_05026_io_exchange GROUP BY k)
        SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0" \
    2>&1 | client_total StreamingExchangeReceiveBytes)
echo "distributed plan gather streams StreamingExchangeReceiveBytes to the client: $(( ${receive_bytes:-0} > 0 ))"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05026_io_exchange"
