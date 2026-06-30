#!/usr/bin/env bash

# Regression: a distributed INSERT must not forward initiator-only settings to the shards.
# `DistributedSink` used to strip only `database` before sending the per-shard INSERT settings, so
# the new format settings (`input_format` / `output_format` / `default_format` / `compression`) and
# the query-construction settings (`offset` / `limit` / `page` / `select` / `filter` / ...) leaked to
# the shards. They are irrelevant to a `Native`-block remote INSERT, and on a rolling upgrade an older
# shard rejects the unknown settings with `UNKNOWN_SETTING`. The sink now uses the shared
# `ClusterProxy::stripInitiatorOnlySettings`. This test sets such settings on a distributed INSERT and
# checks the shard-side (secondary) INSERT queries no longer carry them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dist"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist AS dst ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, dst, x)"

QUERY_ID="04424-${CLICKHOUSE_DATABASE}-$$"

# `prefer_localhost_replica = 0` forces the remote-connection path (the one that strips settings
# before sending); `distributed_foreground_insert = 1` makes the shard INSERTs run synchronously so
# they are logged before we flush. `offset` (a construction setting) is passed as a session setting
# via the command line — it cannot sit on the INSERT statement (rejected on a write query) — while the
# format settings sit on the statement.
${CLICKHOUSE_CLIENT} --offset=2 --query_id="${QUERY_ID}" -q "
    INSERT INTO dist
    SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0, log_queries = 1,
             input_format = 'CSV', output_format = 'CSV', default_format = 'CSV', compression = 'gz'
    SELECT number FROM numbers(10)"

echo "-- data fully landed on the shards"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(x) FROM dst"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

echo "-- at least one shard-side INSERT ran, and none of them carry the initiator-only settings"
${CLICKHOUSE_CLIENT} -q "
    SELECT
        count() >= 1 AS ran_on_shards,
        countIf(Settings['input_format'] != '') AS leaked_input_format,
        countIf(Settings['output_format'] != '') AS leaked_output_format,
        countIf(Settings['default_format'] != '') AS leaked_default_format,
        countIf(Settings['compression'] != '') AS leaked_compression,
        countIf(Settings['offset'] != '') AS leaked_offset
    FROM system.query_log
    WHERE initial_query_id = '${QUERY_ID}'
      AND is_initial_query = 0
      AND query_kind = 'Insert'
      AND type = 'QueryFinish'
      AND event_date >= today() - 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE dist"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
