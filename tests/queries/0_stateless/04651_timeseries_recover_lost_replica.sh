#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree, no-fasttest
# Tag zookeeper: needs Keeper both for the Replicated database engine and for the
#                keeper-client surgery that makes the local metadata diverge from Keeper.
# Tag no-parallel: rewrites Keeper nodes under the database path and forces the replica
#                  through recovery, which detaches and re-creates tables.
# Tag no-replicated-database: the test creates its own Replicated database and forces
#                             recovery on it; the runner's --replicated-database wrapper
#                             would wrap it in a second one (same reason as 04340/03920).
# Tag no-shared-merge-tree: SharedMergeTree-backed inner tables use a different drop path
#                           (same reason as 04341).
# Tag no-fasttest: Fast test has no Keeper (same reason as 04341).
#
# Control and regression cases for diverged `TimeSeries` tables with external and inner targets.
# The external control uses bucketed samples; the inner regression preserves the version 7 row
# layout. Version 8 inner-target recovery is covered separately.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rdb"
ZK_PATH="/test/timeseries_recover_lost_replica/${CLICKHOUSE_TEST_UNIQUE_NAME}"

CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

# Force the replica to consider itself lost without waiting out logs_to_keep: 42 is the sentinel
# digest the server itself writes (DatabaseReplicatedDDLWorker::FORCE_AUTO_RECOVERY_DIGEST) before
# triggering automatic recovery. Re-attaching then re-runs initializeReplication, which sees
# lost_according_to_digest and calls recoverLostReplica.
function force_recovery()
{
    ${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/replicas/s1|r1/digest' '42'"
    ${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DB}"
    ${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}"
}

# Make the local metadata of $1 diverge from Keeper, which is what puts it into
# `tables_to_detach` during recovery. ALTER ... MODIFY COMMENT updates both the local metadata and
# the Keeper node, so afterwards we restore the ORIGINAL definition into Keeper only: the local
# copy keeps the comment, Keeper does not, and recovery sees a mismatch. The captured value must
# be re-escaped for keeper-client, whose parser otherwise strips the single quotes around the UUID
# and leaves an unparseable definition in Keeper.
function diverge_from_keeper()
{
    local table="$1"
    local zk_meta_orig
    zk_meta_orig=$(${CLICKHOUSE_KEEPER_CLIENT} -q "get '${ZK_PATH}/metadata/${table}'" | sed "s/'/\\\\'/g")
    ${CLIENT} -q "ALTER TABLE ${DB}.${table} MODIFY COMMENT 'diverged'"
    ${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/metadata/${table}' '${zk_meta_orig}'"
}

# Recovery rewrites the replica digest as its last step.
function recovered()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT value != '42' FROM system.zookeeper WHERE path = '${ZK_PATH}/replicas/s1|r1' AND name = 'digest'"
}

# Recovery runs asynchronously after ATTACH DATABASE. 240 * 0.5 s leaves an unfixed build inside the runner timeout,
# so it reports a reference diff instead of being killed.
function wait_for_recovery()
{
    for _ in {1..240}; do
        [ "$(recovered)" = "1" ] && return
        sleep 0.5
    done
}

# Path-specific oracle: count the inner-drop rejections attributed to one table. Scoped to the
# DatabaseCatalog logger and to this run's database name so the queries this test itself issues
# (which executeQuery logs verbatim, message text included) cannot satisfy the predicate.
# max_rows_to_read = 0 because system.text_log is large and the test profile caps row reads.
function count_inner_drop_rejections()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.text_log
        WHERE logger_name = 'DatabaseCatalog'
          AND event_time >= toDateTime(${TEST_START})
          AND message LIKE '%${DB}.$1 %'
          AND message LIKE '%ON CLUSTER is not allowed for Replicated database%'
        SETTINGS max_rows_to_read = 0"
}

# The log checks read only this run's part of the server-wide text_log.
TEST_START=$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp(now())")
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"

# ---------------------------------------------------------------------------------------------
# A `TimeSeries` table with EXTERNAL target tables. It still owns the on-by-default recent
# samples table, so `dropInnerTableIfAny` adds its drop to the metadata transaction.
# This shape already recovered before the fix, so
# it is the control case.
# ---------------------------------------------------------------------------------------------

${CLIENT} -q "CREATE TABLE ${DB}.ext_data (id UUID,
                  samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
                  bucket DateTime64(3),
                  min_time SimpleAggregateFunction(min, DateTime64(3)),
                  max_time SimpleAggregateFunction(max, DateTime64(3)))
              ENGINE = ReplicatedAggregatingMergeTree ORDER BY (id, bucket)"
${CLIENT} -q "CREATE TABLE ${DB}.ext_tags (
                  id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
                  metric_name LowCardinality(String),
                  tags Map(LowCardinality(String), String),
                  all_tags Map(String, String),
                  min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
                  max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
              ENGINE = ReplicatedAggregatingMergeTree PRIMARY KEY metric_name
              ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1"
${CLIENT} -q "CREATE TABLE ${DB}.ext_metrics (metric_family String, type LowCardinality(String),
                                             unit LowCardinality(String), help String)
              ENGINE = ReplicatedReplacingMergeTree ORDER BY metric_family"
${CLIENT} --allow_experimental_time_series_table=1 \
    -q "CREATE TABLE ${DB}.ts_ext ENGINE = TimeSeries
        DATA ${DB}.ext_data TAGS ${DB}.ext_tags METRICS ${DB}.ext_metrics"

diverge_from_keeper ts_ext
force_recovery
wait_for_recovery

${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts_ext"
# Recovery restored the Keeper-side definition, i.e. the local divergence really was discarded.
${CLICKHOUSE_CLIENT} -q "SELECT comment = '' FROM system.tables WHERE database = '${DB}' AND name = 'ts_ext'"
count_inner_drop_rejections ts_ext

# ---------------------------------------------------------------------------------------------
# Part 2: a `TimeSeries` table with INNER target tables -- the shape that used to wedge recovery.
# ---------------------------------------------------------------------------------------------

${CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${DB}.ts ENGINE = TimeSeries
    SETTINGS version = 7, recent_samples_ttl_seconds = 345600"

# The outer `TimeSeries` table, its 4 inner targets (samples, recent samples, tags, metric families),
# and the recent samples target of the external control: 6 tables in total.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"

# Fill one inner table, so that afterwards the inner tables can be shown to be genuinely NEW rather
# than the pre-existing ones left in place. The inner table names cannot show this: they embed the
# OUTER table's UUID (getTimeSeriesInnerTableName), which recovery preserves, and neither can their
# own UUIDs -- the Keeper CREATE carries explicit `<KIND> INNER UUID` clauses and recovery replays it
# as an ATTACH, so the inner UUIDs are preserved too. The row count is the discriminator: the drop
# and re-create leaves the inner tables empty, so a regression that made the eager inner drop a
# silent no-op would leave these rows in place.
SAMPLES_TABLE=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.samples.%'")
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.\`${SAMPLES_TABLE}\` (timestamp, value) SELECT now64(3), number FROM numbers(50)"
${CLICKHOUSE_CLIENT} -q "SELECT sum(total_rows) FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.%'"

diverge_from_keeper ts
force_recovery
wait_for_recovery

# Before the fix this stays at 5: only the inner targets survive, `ts` never comes back.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"
# `ts` specifically exists again (a count of 6 made only of inner tables would be wrong).
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"
${CLICKHOUSE_CLIENT} -q "SELECT comment = '' FROM system.tables WHERE database = '${DB}' AND name = 'ts'"
# The inner tables really were dropped and re-created, so they are empty again. If the eager inner
# drop silently did nothing, the 50 rows inserted above would still be here.
${CLICKHOUSE_CLIENT} -q "SELECT sum(total_rows) FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.%'"

# Vacuity guard: assert recovery actually ran and actually took the DROP branch for `ts`. Without
# this the assertions above would also pass on a run where recovery never triggered at all.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() > 0
    FROM system.text_log
    WHERE logger_name = 'DatabaseReplicated (${DB})'
      AND event_time >= toDateTime(${TEST_START})
      AND message LIKE 'Will DROP TABLE ts,%'
    SETTINGS max_rows_to_read = 0"

count_inner_drop_rejections ts

# Best-effort, time-bounded cleanup. On an unfixed build the wedged inner drop never completes, so
# DROP DATABASE blocks; without the bound the test would be killed by the runner timeout instead of
# reporting its reference diff. Recovery also creates the two side databases it exiles the
# data-bearing tables into (DatabaseReplicated::BROKEN_TABLES_SUFFIX and
# BROKEN_REPLICATED_TABLES_SUFFIX), so drop those too rather than leaving them behind.
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC" 2>/dev/null || true
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_replicated_tables SYNC" 2>/dev/null || true
