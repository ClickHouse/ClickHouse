#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The fixed-key aggregation state precomputes per-row hashes from its batch-packed keys.
# `Aggregator::executeImplUntilAdaptiveFreeze` shares one such state across the whole block but
# aggregates it slice by slice and may hand the tail over to another state once the local table
# crosses the freeze threshold, so the hashes are produced in chunks as the block is consumed
# rather than in one pass over the whole block. Check that the chunked window keeps the results
# exact: a stale window would reuse a hash that does not belong to the row and split or merge groups.

settings="enable_software_prefetch_in_aggregation = 1, max_threads = 4, adaptive_aggregator_freeze_threshold = 500000, adaptive_aggregator_freeze_threshold_bytes = 1000000000"

query="
    SELECT count(), sum(a), sum(b), sum(c), max(c)
    FROM
    (
        SELECT a, b, count() AS c
        FROM
        (
            SELECT number % 1000000 AS a, toUInt64(number % 37) AS b
            FROM numbers_mt(4000000)
        )
        GROUP BY a, b
    )"

# The adaptive learning path and the plain path must agree, key for key.
adaptive=$($CLICKHOUSE_CLIENT -q "$query SETTINGS $settings, enable_adaptive_aggregator = 1")
plain=$($CLICKHOUSE_CLIENT -q "$query SETTINGS $settings, enable_adaptive_aggregator = 0")

echo "$adaptive"
[ "$adaptive" = "$plain" ] && echo 'adaptive matches plain'

# `AggregationPrecomputedFixedKeyHashes` counts the rows the fixed-key states actually hashed.
# Hashing the whole block upfront makes a learning state pay for rows it never looks up, so the
# count must stay within the rows the query read (plus one chunk per state for the last, partly
# consumed chunk). A machine whose L2 cache is larger than the hash table skips the precomputed-hash
# path altogether and reports zero, which trivially satisfies the bound.
query_id="fixed-key-prefetch-adaptive-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "$query SETTINGS $settings, enable_adaptive_aggregator = 1" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['AggregationPrecomputedFixedKeyHashes'] <= 1.5 * ProfileEvents['SelectedRows']
    FROM system.query_log
    WHERE query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
