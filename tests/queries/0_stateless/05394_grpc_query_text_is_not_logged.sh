#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built
# Tag no-replicated-database: the replicas listen on the same gRPC port, so the query can run on a server whose text_log is not read

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

key=$(${CLICKHOUSE_CLIENT} --query "SELECT hex(randomString(8))")
query="SELECT hex(encrypt('aes-128-ecb', 'x', '${key}'))"
query_id="${CLICKHOUSE_DATABASE}_grpc_query_text"
probe_id="${CLICKHOUSE_DATABASE}_grpc_query_text_probe"
test_start=$(${CLICKHOUSE_CLIENT} --query "SELECT toUnixTimestamp(now())")

python3 - "$CURDIR/../../../utils/grpc-client/pb2" "$query" "$query_id" <<'EOF'
import sys
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

with grpc.insecure_channel("localhost:9100") as channel:
    result = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel).ExecuteQuery(
        clickhouse_grpc_pb2.QueryInfo(query=sys.argv[2], query_id=sys.argv[3]))
    print(not result.HasField("exception"))
EOF

for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    landed=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() > 0 FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
          AND message = 'Received initial QueryInfo: query: ${#query} bytes, query_id: ${query_id}'
        SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0")
    [ "$landed" = "1" ] && break
    sleep 0.5
done
echo "$landed"

# The probe's own log lines contain the key, so they are excluded by its query id.
${CLICKHOUSE_CLIENT} --query_id "${probe_id}" --query "
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND query_id != '${probe_id}' AND position(message, '${key}') > 0
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"
