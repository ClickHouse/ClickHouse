#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A cached row count skips the data `GET` whose response headers are the only source of `_headers`,
# so a query reading `_headers` must not take the count shortcut. The `hit` query is the control: it
# shows the cached row count IS servable for this URI, so a declined lookup is a decision and not an
# empty cache. All three statements carry the same settings so they share one schema-cache key.
# `optimize_trivial_count_query = 0` alone stops `count()` from reaching the row-count lookup, which
# would make the control read as a declined one, so pin it with the rest of the shortcut settings.
# `enable_parallel_replicas` over a multi-replica cluster rewrites the read into `urlCluster`, moving
# the lookup to a replica whose `query_log` row carries a different `query_id` than the one read
# below, so pin that off too and keep all three statements on the local read path.

SET="optimize_count_from_files = 1, use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0, optimize_trivial_count_query = 1, enable_parallel_replicas = 0"
URI="http://127.0.0.1:8123/?query=select+5212&user=default"

WARM="${CLICKHOUSE_TEST_UNIQUE_NAME}_warm"
HIT="${CLICKHOUSE_TEST_UNIQUE_NAME}_hit"
GUARDED="${CLICKHOUSE_TEST_UNIQUE_NAME}_guarded"

$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$WARM" -q "
SELECT count() FROM url('$URI', LineAsString, 's String') SETTINGS $SET"

$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$HIT" -q "
SELECT count() FROM url('$URI', LineAsString, 's String') SETTINGS $SET FORMAT Null"

$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$GUARDED" -q "
SELECT min(mapContains(_headers, 'X-ClickHouse-Query-Id')) FROM url('$URI', LineAsString, 's String') SETTINGS $SET"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

for id in "$HIT" "$GUARDED"; do
    $CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['SchemaInferenceCacheNumRowsHits'] FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id = '$id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
done
