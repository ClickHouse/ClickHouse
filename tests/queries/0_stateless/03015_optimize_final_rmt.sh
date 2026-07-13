#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings, no-tsan, no-msan, no-ubsan, no-asan, no-debug
# no sanitizers: too slow

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE 03015_optimize_final_rmt(a UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/03015_optimize_final_rmt', 'r1') ORDER BY a SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=100"

for _ in {0..10}; do
  ${CLICKHOUSE_CLIENT} --insert_deduplicate 0 -q "INSERT INTO 03015_optimize_final_rmt select * from numbers_mt(1e6)"
done

# trigger a merge if it is not already running
# timeout for each individual attempt to wait until queue is drained is receive_timeout/10, so it makes sense to increase its value to not get timeout exception
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE 03015_optimize_final_rmt FINAL SETTINGS receive_timeout=3000" &

# this query should wait for the running merges, not just return immediately
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE 03015_optimize_final_rmt FINAL SETTINGS receive_timeout=3000"

# then at this point we should have a single part
${CLICKHOUSE_CLIENT} -q "SELECT COUNT() FROM system.parts WHERE database = currentDatabase() AND table = '03015_optimize_final_rmt' AND active"

wait

${CLICKHOUSE_CLIENT} --query "DROP TABLE 03015_optimize_final_rmt"
