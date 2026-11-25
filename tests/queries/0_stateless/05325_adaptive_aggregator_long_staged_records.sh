#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregation stages the rows its frozen tables miss in chunks carved one after another from shared blocks,
# and a chunk that opens with a record larger than the doubled capacity of the previous one is sized by that record.
# The records are 4-byte aligned only, while every chunk starts with an 8-byte aligned header, so such a chunk must not
# leave the next one carved after it misaligned. The cells stage records of a few kilobytes: a count over String keys
# of 2004 bytes each, whose records end 4 bytes past a multiple of 8, the same over keys of many lengths, and a
# general payload whose String argument is a few kilobytes long. Every cell compares the result with the one of the
# feature off and says whether records were staged; each runs in its own clickhouse-local process, so the counter in
# `system.events` belongs to it alone.

function check()
{
    local label=$1 query=$2
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 2, max_block_size = 1024, adaptive_aggregator_freeze_threshold = 16, enable_adaptive_aggregator = 1;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT '$label',
        (SELECT sum(cityHash64(*)) FROM ($query)) = (SELECT sum(cityHash64(*)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
    " | paste -sd '\t'
}

check 'Keys of 2004 bytes' \
    "SELECT k, count() AS c FROM (SELECT concat(toString(number % 5000 + 10000), repeat('x', 1999)) AS k FROM numbers_mt(10000)) GROUP BY k"
check 'Keys of many lengths' \
    "SELECT k, count() AS c FROM (SELECT repeat(toString(number % 5000), 1 + number % 700) AS k FROM numbers_mt(10000)) GROUP BY k"
check 'Long String argument' \
    "SELECT toUInt64(number % 5000) AS k, max(repeat('y', 2000 + number % 13)) AS m FROM numbers_mt(10000) GROUP BY k"
