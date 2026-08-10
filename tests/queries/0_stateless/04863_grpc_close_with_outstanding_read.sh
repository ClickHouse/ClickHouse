#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [[ -z "$CLICKHOUSE_GRPC_CLIENT" ]]; then
  CLICKHOUSE_GRPC_CLIENT="$CURDIR/../../../utils/grpc-client/clickhouse-grpc-client.py"
fi

# ExecuteQueryWithStreamIO keeps a speculative read outstanding for the whole query, so each
# of these calls tears down a responder that gRPC may still hold a completion queue tag into.
for _ in {1..10}; do
  $CLICKHOUSE_GRPC_CLIENT --query "SELECT 1" > /dev/null
done

# Streaming output with many intermediate writes, terminated by an exception: same teardown
# reached from the error path. The row count before the exception is not deterministic.
$CLICKHOUSE_GRPC_CLIENT --query "SELECT throwIf(number = 5000) FROM numbers(10000) SETTINGS max_block_size = 100" > /dev/null 2>&1

$CLICKHOUSE_GRPC_CLIENT --query "SELECT 'alive'"
