#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator freezes a thread's local table at whichever of the key-count and the
# byte thresholds is reached first. Each query pins both thresholds explicitly (the runner's
# settings randomization must not move the freeze point) and the assertions run in freeze-free
# order first, so the event counters stay absolute. Everything runs in one clickhouse-local
# process, so the counters in `system.events` belong to these queries alone. The hash-table
# statistics are disabled because the queries share a statistics key: the never-freezing first
# query would otherwise leave a size hint that makes the later runs initialize two-level, and a
# two-level table cannot freeze (the artificial key threshold here is far above the hint, so
# the single-level cap that protects real thresholds does not apply).
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET enable_adaptive_aggregator = 1;
SET group_by_two_level_threshold = 100000;
SET collect_hash_table_stats_during_aggregation = 0;

-- The byte bound disabled with an unreachable key bound: the tables must never freeze.
SELECT intHash64(number) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k FORMAT Null
SETTINGS adaptive_aggregator_freeze_threshold = 1000000000, adaptive_aggregator_freeze_threshold_bytes = 0;
SELECT 'no freeze when disabled', count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes' AND value > 0;

-- A tiny byte bound with the same unreachable key bound: the tables must freeze by bytes.
SELECT intHash64(number) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k FORMAT Null
SETTINGS adaptive_aggregator_freeze_threshold = 1000000000, adaptive_aggregator_freeze_threshold_bytes = 65536;
SELECT 'freezes by bytes', count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes' AND value > 0;

-- The result of a byte-triggered freeze matches the ordinary aggregation.
SELECT 'exact', count(), sum(c), sum(k % 1000 = 0 ? c : 0) FROM (
    SELECT intHash64(number) % 500000 AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k
    SETTINGS adaptive_aggregator_freeze_threshold = 1000000000, adaptive_aggregator_freeze_threshold_bytes = 65536
);
SELECT 'exact', count(), sum(c), sum(k % 1000 = 0 ? c : 0) FROM (
    SELECT intHash64(number) % 500000 AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k
    SETTINGS enable_adaptive_aggregator = 0
);
"
