#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: the test creates two replicas of an external samples table.
# Tag no-shared-merge-tree: the test checks leader maintenance of `ReplicatedMergeTree` partitions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/05044_recent_samples"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ext_samples_r1
    (
        id Tuple(UInt64, UUID),
        timestamp DateTime64(3),
        value Float64
    )
    ENGINE = ReplicatedMergeTree('${ZK_PATH}/samples', 'r1')
    ORDER BY (id, timestamp)"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ts_recent_r1 ENGINE = TimeSeries
    SETTINGS recent_samples_ttl_seconds = 3600, recent_samples_partition_by = 'toStartOfHour(timestamp)'
    SAMPLES ext_samples_r1
    RECENT SAMPLES ENGINE = ReplicatedMergeTree('${ZK_PATH}/recent', 'r1')
        PARTITION BY toStartOfHour(timestamp) ORDER BY (id, timestamp)"

RECENT_R1=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '.inner_id.recentsamples.%' AND engine_full LIKE '%${ZK_PATH}/recent%'")

${CLICKHOUSE_CLIENT} -q "ALTER TABLE \`${RECENT_R1}\` MODIFY TTL toDateTime(timestamp) + toIntervalSecond(1)
    SETTINGS materialize_ttl_after_modify = 0"
for _ in {1..5}; do
    query_with_retry "SELECT throwIf(engine_full LIKE '% TTL %') FROM system.tables
        WHERE database = currentDatabase() AND name = '${RECENT_R1}'" >/dev/null
done
${CLICKHOUSE_CLIENT} -q "SELECT throwIf(engine_full LIKE '% TTL %') FROM system.tables
    WHERE database = currentDatabase() AND name = '${RECENT_R1}'" >/dev/null

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ext_samples_r2
    (
        id Tuple(UInt64, UUID),
        timestamp DateTime64(3),
        value Float64
    )
    ENGINE = ReplicatedMergeTree('${ZK_PATH}/samples', 'r2')
    ORDER BY (id, timestamp)
    SETTINGS replicated_can_become_leader = 0"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ts_recent_r2 ENGINE = TimeSeries
    SETTINGS recent_samples_ttl_seconds = 3600, recent_samples_partition_by = 'toStartOfHour(timestamp)'
    SAMPLES ext_samples_r2
    RECENT SAMPLES ENGINE = ReplicatedMergeTree('${ZK_PATH}/recent', 'r2')
        PARTITION BY toStartOfHour(timestamp) ORDER BY (id, timestamp)
        SETTINGS replicated_can_become_leader = 0"

RECENT_R2=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '.inner_id.recentsamples.%' AND engine_full LIKE '%${ZK_PATH}/recent%'
    AND name != '${RECENT_R1}'")

${CLICKHOUSE_CLIENT} -q "
    SELECT is_leader FROM system.replicas WHERE database = currentDatabase() AND table = '${RECENT_R1}';
    SELECT is_leader FROM system.replicas WHERE database = currentDatabase() AND table = '${RECENT_R2}'"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    INSERT INTO ts_recent_r2 (metric_name, tags, time_series) VALUES
    ('external_metric', map(), [(toDateTime64('2000-01-01 00:00:00', 3), 1.)]),
    ('external_metric', map(), [(toDateTime64('2000-01-01 10:00:00', 3), 2.)])"

${CLICKHOUSE_CLIENT} -q "SYSTEM SYNC REPLICA ext_samples_r1; SYSTEM SYNC REPLICA \`${RECENT_R1}\`"
${CLICKHOUSE_CLIENT} -q "SELECT countDistinct(partition) FROM system.parts
    WHERE database = currentDatabase() AND table = '${RECENT_R1}' AND active"

for _ in {1..5}; do
    query_with_retry "SELECT throwIf(countDistinct(partition) != 1) FROM system.parts
        WHERE database = currentDatabase() AND table = '${RECENT_R1}' AND active" >/dev/null
done
${CLICKHOUSE_CLIENT} -q "SELECT throwIf(countDistinct(partition) != 1) FROM system.parts
    WHERE database = currentDatabase() AND table = '${RECENT_R1}' AND active" >/dev/null

${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(timestamp = toDateTime64('2000-01-01 00:00:00', 3)),
           countIf(timestamp = toDateTime64('2000-01-01 10:00:00', 3))
    FROM \`${RECENT_R1}\`"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ts_recent_r2 SYNC; DROP TABLE ts_recent_r1 SYNC; DROP TABLE ext_samples_r2 SYNC; DROP TABLE ext_samples_r1 SYNC"
