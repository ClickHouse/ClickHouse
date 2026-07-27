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
# Regression test for: a diverged TimeSeries table wedges DatabaseReplicated::recoverLostReplica
# forever, so the replica never finishes recovery and the database stays unusable.
#
# Recovery drops a table that does not store data on disk (DatabaseReplicated.cpp, the
# `drop_broken_tables || !table->storesDataOnDisk()` branch). StorageTimeSeries has no
# storesDataOnDisk override, so a diverged TimeSeries table takes that branch. The inner-table
# special case right below it used to name only MaterializedView and WindowView, and it is the
# only place in recovery that hands a ZooKeeperMetadataTransaction to the inner DROP. Without it
# the inner DROP was deferred to the background dropTableFinally task, which has no transaction,
# so the DROP was re-routed into the replicated DDL log and rejected with
# `It's not initial query. ON CLUSTER is not allowed for Replicated database.` (Code 80) forever,
# and waitTableFinallyDropped never returned.
#
# Expected (after fix):  the replica recovers, the TimeSeries table and its 3 inner tables are
#                        back, and no Code 80 inner-drop rejection is logged.
# Observed (before fix): recovery never completes -- the table count stays at 3 (only the
#                        orphaned inner tables), `ts` is gone, and DatabaseCatalog logs
#                        `Cannot drop table <db>.ts (<uuid>). Will retry later.: Code: 80 ...`
#                        every 5 seconds.
#
# The external-target part runs FIRST on purpose: it needs the replicated DDL queue, and once the
# inner-target part has wedged recovery on an unfixed build that queue no longer drains, so the
# reverse order would make an unfixed build sit in DDL timeouts instead of failing on the
# reference diff.

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

# Recovery is asynchronous, so poll (bounded) until the table is back.
function wait_for_table()
{
    local table="$1"
    for _ in {1..120}; do
        [ "$(${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.${table}")" = "1" ] && return
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
          AND message LIKE '%${DB}.$1 %'
          AND message LIKE '%ON CLUSTER is not allowed for Replicated database%'
        SETTINGS max_rows_to_read = 0"
}

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"

# ---------------------------------------------------------------------------------------------
# Part 1: a TimeSeries table with EXTERNAL target tables, so hasInnerTables() is false,
# dropInnerTableIfAny returns without adding any operation, and the metadata transaction is
# committed with an empty operation list. This shape already recovered before the fix, so it is
# the control that shows the fix did not change the no-inner-tables path.
# ---------------------------------------------------------------------------------------------

${CLIENT} -q "CREATE TABLE ${DB}.ext_data (id UUID, timestamp DateTime64(3), value Float64)
              ENGINE = ReplicatedMergeTree ORDER BY (id, timestamp)"
${CLIENT} -q "CREATE TABLE ${DB}.ext_tags (
                  id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
                  metric_name LowCardinality(String),
                  tags Map(LowCardinality(String), String),
                  all_tags Map(String, String),
                  min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
                  max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
              ENGINE = ReplicatedAggregatingMergeTree PRIMARY KEY metric_name
              ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1"
${CLIENT} -q "CREATE TABLE ${DB}.ext_metrics (metric_family_name String, type LowCardinality(String),
                                             unit LowCardinality(String), help String)
              ENGINE = ReplicatedReplacingMergeTree ORDER BY metric_family_name"
${CLIENT} --allow_experimental_time_series_table=1 \
    -q "CREATE TABLE ${DB}.ts_ext ENGINE = TimeSeries
        DATA ${DB}.ext_data TAGS ${DB}.ext_tags METRICS ${DB}.ext_metrics"

diverge_from_keeper ts_ext
force_recovery
wait_for_table ts_ext

${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts_ext"
# Recovery restored the Keeper-side definition, i.e. the local divergence really was discarded.
${CLICKHOUSE_CLIENT} -q "SELECT comment = '' FROM system.tables WHERE database = '${DB}' AND name = 'ts_ext'"
count_inner_drop_rejections ts_ext

# ---------------------------------------------------------------------------------------------
# Part 2: a TimeSeries table with INNER target tables -- the shape that used to wedge recovery.
# ---------------------------------------------------------------------------------------------

${CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${DB}.ts ENGINE = TimeSeries"

# The outer TimeSeries table plus its 3 inner tables (data, tags, metrics).
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"

diverge_from_keeper ts
force_recovery
wait_for_table ts

# Before the fix this stays at 3: only the orphaned inner tables survive, `ts` never comes back.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"
# `ts` specifically exists again (a count of 4 made only of inner tables would be wrong).
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"
${CLICKHOUSE_CLIENT} -q "SELECT comment = '' FROM system.tables WHERE database = '${DB}' AND name = 'ts'"

# Vacuity guard: assert recovery actually ran and actually took the DROP branch for `ts`. Without
# this the assertions above would also pass on a run where recovery never triggered at all.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() > 0
    FROM system.text_log
    WHERE logger_name = 'DatabaseReplicated (${DB})'
      AND message LIKE 'Will DROP TABLE ts,%'
    SETTINGS max_rows_to_read = 0"

count_inner_drop_rejections ts

# Best-effort, time-bounded cleanup. On an unfixed build the wedged inner drop never completes, so
# DROP DATABASE blocks; without the bound the test would be killed by the runner timeout instead of
# reporting its reference diff.
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
