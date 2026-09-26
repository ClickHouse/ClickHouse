#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree, no-fasttest
# Tag zookeeper: needs Keeper for the Replicated database and keeper-client metadata surgery.
# Tag no-parallel: rewrites Keeper nodes and forces database recovery.
# Tag no-replicated-database: the test creates its own `Replicated` database.
# Tag no-shared-merge-tree: `SharedMergeTree`-backed inner tables use a different drop path.
# Tag no-fasttest: Fast test has no Keeper.
#
# Regression test for a diverged `TimeSeries` table with INNER target tables. Recovery drops the
# outer table before re-creating it, and must drop its inner tables in the same metadata
# transaction. Without that transaction the inner `DROP` is deferred, re-routed into the replicated
# `DDL` log, and rejected with `ON CLUSTER is not allowed for Replicated database` (Code 80).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rdb_inner"
ZK_PATH="/test/timeseries_recover_lost_replica_inner/${CLICKHOUSE_TEST_UNIQUE_NAME}"

CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

# 42 is the sentinel digest that triggers automatic recovery. Re-attaching re-runs
# `initializeReplication`, which sees the digest and calls `recoverLostReplica`.
function force_recovery()
{
    ${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/replicas/s1|r1/digest' '42'"
    ${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DB}"
    ${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}"
}

# Change local metadata, then restore only its original Keeper definition. This leaves the local
# copy with a comment and makes recovery detach it as diverged. Escape quotes for keeper-client.
function diverge_from_keeper()
{
    local table="$1"
    local zk_meta_orig
    zk_meta_orig=$(${CLICKHOUSE_KEEPER_CLIENT} -q "get '${ZK_PATH}/metadata/${table}'" | sed "s/'/\\\\'/g")
    ${CLIENT} -q "ALTER TABLE ${DB}.${table} MODIFY COMMENT 'diverged'"
    ${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/metadata/${table}' '${zk_meta_orig}'"
}

# `recoverLostReplica` logs this after the create loop has finished. An outer table can reappear
# before its inner tables because their order within the dependency level is unspecified.
function count_recovery_completions()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.text_log
        WHERE logger_name = 'DatabaseReplicated (${DB})'
          AND message = 'All tables are created successfully'
        SETTINGS max_rows_to_read = 0"
}

# Wait for both a new completed recovery and the restored Keeper-side definition. Mere table
# existence would also match the original diverged table. 240 * 0.5 s = 120 s.
# $1 = table, $2 = completion count captured before forcing recovery
function wait_for_recovery()
{
    local table="$1"
    local base="$2"
    for _ in {1..240}; do
        if [ "$(count_recovery_completions)" -gt "$base" ] \
           && [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
                     WHERE database = '${DB}' AND name = '${table}' AND comment = ''")" = "1" ]; then
            return
        fi
        sleep 0.5
    done
}

# Path-specific oracle for inner-drop rejections. Scope to `DatabaseCatalog` and this database so
# queries logged verbatim by `executeQuery` cannot satisfy the predicate.
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

${CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${DB}.ts ENGINE = TimeSeries"

# The outer `TimeSeries` table plus its 4 inner targets (samples, recent samples, tags, metrics).
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"

# Fill one inner table, so that afterwards the inner tables can be shown to be genuinely NEW rather
# than the pre-existing ones left in place. The inner table names cannot show this: they embed the
# OUTER table's UUID (`getTimeSeriesInnerTableName`), which recovery preserves, and neither can their
# own UUIDs -- the Keeper `CREATE` carries explicit `<KIND> INNER UUID` clauses and recovery replays it
# as an `ATTACH`, so the inner UUIDs are preserved too. The row count is the discriminator: the drop
# and re-create leaves the inner tables empty, so a regression that made the eager inner drop a
# silent no-op would leave these rows in place.
SAMPLES_TABLE=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.samples.%'")
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.\`${SAMPLES_TABLE}\` (bucket) SELECT toDateTime(number) FROM numbers(50)"
${CLICKHOUSE_CLIENT} -q "SELECT sum(total_rows) FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.%'"

RECOVERIES_BEFORE=$(count_recovery_completions)
diverge_from_keeper ts
force_recovery
wait_for_recovery ts "$RECOVERIES_BEFORE"

# Before the fix this stays at 4: only the orphaned inner tables survive, `ts` never comes back.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND (name = 'ts' OR name LIKE '.inner_id.%')"
# `ts` specifically exists again (a count of 5 made only of inner tables would be wrong).
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"
${CLICKHOUSE_CLIENT} -q "SELECT comment = '' FROM system.tables WHERE database = '${DB}' AND name = 'ts'"
# The inner tables really were dropped and re-created, so they are empty again. If the eager inner
# drop silently did nothing, the 50 rows inserted above would still be here.
${CLICKHOUSE_CLIENT} -q "SELECT sum(total_rows) FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.%'"
# Recovery may attach the outer table before its inner targets. Once recovery completes, target
# access must still validate and resolve the restored samples table, not merely leave `ts` visible.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM timeSeriesSamples('${DB}', 'ts')"

# Vacuity guard: assert recovery actually ran and actually took the `DROP` branch for `ts`. Without
# this the assertions above would also pass on a run where recovery never triggered at all.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() > 0
    FROM system.text_log
    WHERE logger_name = 'DatabaseReplicated (${DB})'
      AND message LIKE 'Will DROP TABLE ts,%'
    SETTINGS max_rows_to_read = 0"

count_inner_drop_rejections ts

# Best-effort, time-bounded cleanup. On an unfixed build the wedged inner drop can block
# `DROP DATABASE`; keep the test within its runner timeout. Recovery may also create side databases
# for data-bearing tables (`DatabaseReplicated::BROKEN_TABLES_SUFFIX` and
# `BROKEN_REPLICATED_TABLES_SUFFIX`).
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC" 2>/dev/null || true
timeout 30 ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_replicated_tables SYNC" 2>/dev/null || true
