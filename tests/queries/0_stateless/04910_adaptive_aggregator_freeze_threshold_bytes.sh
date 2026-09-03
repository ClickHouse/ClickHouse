#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator freezes a thread's local table at whichever of the key-count and the
# byte thresholds is reached first. Each query pins both thresholds explicitly, because the
# runner's settings randomization must not move the freeze point. The no-freeze cell runs
# first, while the freeze counter is still zero, so every assertion can test the counter's
# absolute value instead of a delta. Everything runs in one clickhouse-local process, so the
# counters in `system.events` belong to these queries alone. The hash-table statistics are
# disabled so every cell's behavior is a pure function of its own settings: the statistics
# are process-global, and a verdict or size recorded by one cell could otherwise reach the
# admission or initialization of a later one.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET enable_adaptive_aggregator = 1;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 500000000;
SET max_block_size = 65536;
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

# A warm run must freeze the same way a cold run does. This cell deliberately enables the
# hash-table statistics: the second run of the query receives a size hint recorded by the first,
# and the engaged initialization must not let that hint move the freeze point. The key threshold
# is unreachable, and the 2 MiB byte bound takes tens of 4096-row blocks of genuine growth to
# cross, so every legitimate freeze holds well over 32768 keys (the hash buffer alone must
# outgrow the bound). A table pre-sized from the hint would instead freeze at its first
# between-blocks check with about one block of keys, failing the size assertion; a table the
# hint initialized two-level could never freeze at all, failing the count assertion. The
# two-level threshold is pinned below the freeze size so the two-level initialization stays
# reachable from the recorded sizes.
warm_trace="$CLICKHOUSE_TMP/04910_warm_trace.log"
$CLICKHOUSE_LOCAL --send_logs_level=trace --query "
SET max_threads = 4;
SET enable_adaptive_aggregator = 1;
SET group_by_two_level_threshold = 30000;
SET group_by_two_level_threshold_bytes = 500000000;
SET max_block_size = 4096;
SET collect_hash_table_stats_during_aggregation = 1;
SET adaptive_aggregator_freeze_threshold = 1000000000;
SET adaptive_aggregator_freeze_threshold_bytes = 2097152;

SELECT intHash64(number) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k FORMAT Null;
SELECT intHash64(number) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k FORMAT Null;
SELECT 'warm and cold runs all froze', value FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
" 2> "$warm_trace"
grep -o 'frozen at [0-9]*' "$warm_trace" | awk '{ n++; if (n == 1 || $3 < min) min = $3 } END { print "warm freezes by growth\t" (n == 8 && min > 16384 ? 1 : 0) }'
