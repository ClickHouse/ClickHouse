#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so neither grpc nor libfiu is built

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
# reached from the error path. The row count before the exception is not deterministic, so
# only the exception itself is asserted.
$CLICKHOUSE_GRPC_CLIENT --query "SELECT throwIf(number = 5000) FROM numbers(10000) SETTINGS max_block_size = 100" 2>&1 >/dev/null \
  | grep -c -m1 "Value passed to 'throwIf' function is non-zero"

# The natural window in which a read is still outstanding at teardown is tiny, so arm it
# deterministically: with the fail point enabled every close() of a streaming call has one.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT grpc_call_close_with_outstanding_read"
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT grpc_call_close_with_outstanding_read"' EXIT
for _ in {1..5}; do
  $CLICKHOUSE_GRPC_CLIENT --query "SELECT 1" > /dev/null
done
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT grpc_call_close_with_outstanding_read"

$CLICKHOUSE_GRPC_CLIENT --query "SELECT 'alive'"
