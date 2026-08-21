#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
#
# A part whose index was never materialized must not get a no-op replacement when its
# CLEAR INDEX TTL expires.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ttl_clear_index_unmaterialized SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ttl_clear_index_unmaterialized
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS materialize_skip_indexes_on_merge = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO ttl_clear_index_unmaterialized SETTINGS materialize_skip_indexes_on_insert = 0, optimize_on_insert = 0 VALUES ('2000-01-01', 1, 1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ttl_clear_index_unmaterialized SETTINGS materialize_skip_indexes_on_insert = 0, optimize_on_insert = 0 VALUES ('2000-01-01', 2, 2)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ttl_clear_index_unmaterialized FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ttl_clear_index_unmaterialized MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx SETTINGS materialize_ttl_after_modify = 1, mutations_sync = 2"

echo "index_absent:"
${CLICKHOUSE_CLIENT} -q "SELECT secondary_indices_compressed_bytes = 0 FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_unmaterialized' AND active"
echo "ttl_info_present:"
${CLICKHOUSE_CLIENT} -q "SELECT length(index_clear_ttl_info.max) = 1 FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_unmaterialized' AND active"

PART_BEFORE=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_unmaterialized' AND active LIMIT 1")
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ttl_clear_index_unmaterialized FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1, optimize_throw_if_noop = 0"
PART_AFTER=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_unmaterialized' AND active LIMIT 1")
echo "no_replacement:"
if [[ "$PART_BEFORE" == "$PART_AFTER" ]]; then echo 1; else echo 0; fi

echo "check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE ttl_clear_index_unmaterialized SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ttl_clear_index_unmaterialized"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ttl_clear_index_unmaterialized SYNC"
