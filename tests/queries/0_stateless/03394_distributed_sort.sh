#!/usr/bin/env bash
# Tags: no-old-analyzer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test(src_ip UInt32, dst_ip UInt32, bytes UInt64) ENGINE MergeTree() ORDER BY src_ip"

$CLICKHOUSE_CLIENT -q "INSERT INTO test SELECT number%3, number%4, number FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "INSERT INTO test SELECT number%5, number%3, number FROM numbers(10, 10)"

$CLICKHOUSE_CLIENT -q "
SELECT dst_ip, src_ip, bytes
FROM test
WHERE bytes > 5 AND src_ip > 2
ORDER BY dst_ip, src_ip, bytes
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0"

# The WHERE step may appear as either Expression or Filter depending on optimizer settings.
# Normalize to Expression so the test is deterministic with randomized settings.
$CLICKHOUSE_CLIENT -q "
EXPLAIN SELECT dst_ip, src_ip, bytes
FROM test
WHERE bytes > 5 AND src_ip > 2
ORDER BY dst_ip, src_ip, bytes
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_optimize_exchanges=0" | sed 's/Filter ((WHERE/Expression ((WHERE/'

echo '------------------'

$CLICKHOUSE_CLIENT -q "
EXPLAIN SELECT dst_ip, src_ip, bytes
FROM test
WHERE bytes > 5 AND src_ip > 2
ORDER BY dst_ip, src_ip, bytes
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_optimize_exchanges=1" | sed 's/Filter ((WHERE/Expression ((WHERE/'
