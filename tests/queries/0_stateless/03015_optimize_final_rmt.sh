#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE 03015_optimize_final_rmt(a UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/03015_optimize_final_rmt', 'r1') ORDER BY a SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=100"

for _ in {0..10}; do
  ${CLICKHOUSE_CLIENT} --insert_deduplicate 0 -q "INSERT INTO 03015_optimize_final_rmt select * from numbers_mt(1e6)"
done

# trigger a merge if it is not already running
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE 03015_optimize_final_rmt FINAL" &

# this query should wait for the running merges, not just return immediately
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE 03015_optimize_final_rmt FINAL"

# then at this point we should have a single part
${CLICKHOUSE_CLIENT} -q "SELECT COUNT() FROM system.parts WHERE database = currentDatabase() AND table = '03015_optimize_final_rmt' AND active"

wait

${CLICKHOUSE_CLIENT} --query "DROP TABLE 03015_optimize_final_rmt"
