#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

# `ABR` returns the aggregated data of a shard as mergeable state, which the shard merges once more, in the
# memory-efficient mode when `distributed_aggregation_memory_efficient` is enabled. The proxy of a lazily loaded
# `ABR` table is merged in the same mode.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATABASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"
CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --session_timezone=UTC --explain_query_plan_default=legacy
    --distributed_aggregation_memory_efficient=1 --enable_memory_bound_merging_of_aggregation_results=0
    --optimize_aggregation_in_order=0 --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=1"
QUERY="SELECT value % 100 AS k, sum(value) FROM ${DATABASE}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY k"

function check()
{
    for analyzer in 1 0; do
        # The plan of the query on a shard, which reads the `ABR` table up to the mergeable state.
        echo "$1, enable_analyzer = ${analyzer}, memory-efficient: $(${CLIENT} --enable_analyzer=${analyzer} --stage=with_mergeable_state \
            --query "EXPLAIN actions = 1 ${QUERY}" | grep -c 'Mode: memory-efficient')"
        ${CLIENT} --enable_analyzer=${analyzer} --prefer_localhost_replica=0 --query "
            SELECT count(), sum(s) FROM (
                SELECT value % 100 AS k, sum(value) AS s FROM remote('127.0.0.2', '${DATABASE}', 'abr')
                WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY k)"
    done
}

${CLIENT} --query "
    DROP DATABASE IF EXISTS ${DATABASE};
    CREATE DATABASE ${DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

    CREATE TABLE ${DATABASE}.events_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE ${DATABASE}.events_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    INSERT INTO ${DATABASE}.events_1 SELECT toDateTime('2026-01-01 00:00:00') + number, number FROM numbers(10000);

    CREATE TABLE ${DATABASE}.abr ENGINE = ABR('events', [1, 10], ('ts'));
"

check "abr"

# The catalog now returns the `ABR` table through `StorageTableProxy`.
${CLIENT} --query "DETACH DATABASE ${DATABASE}"
${CLIENT} --query "ATTACH DATABASE ${DATABASE}"

check "lazy abr"

${CLIENT} --query "DROP DATABASE ${DATABASE}"
