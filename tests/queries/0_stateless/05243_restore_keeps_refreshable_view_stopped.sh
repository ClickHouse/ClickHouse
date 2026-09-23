#!/usr/bin/env bash
# Tags: no-parallel, atomic-database
# no-parallel: arms a `PAUSEABLE_ONCE` failpoint, which fires once globally, so a concurrent `RESTORE` from
#   another test could steal the pause.
# atomic-database: refreshable materialized views require an `Atomic` database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STOPPED="${CLICKHOUSE_DATABASE}_stopped"
BACKUP_STOPPED="${CLICKHOUSE_TEST_UNIQUE_NAME}_stopped"

# The database lives outside `$CLICKHOUSE_DATABASE`, and the failpoint is server-global: clean both up on every exit.
cleanup() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT restore_pause_before_data_restore_tasks" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$STOPPED\` SYNC"
}
trap cleanup EXIT
cleanup

# A restored refreshable materialized view is held back until the `RESTORE` finishes, so it cannot
# refresh over half-restored data. Finishing the restore must lift only that hold: a view that was
# stopped meanwhile - by `SYSTEM STOP VIEW` here, or by
# `stop_refreshable_materialized_views_on_startup` on a server where it is set - stays stopped.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$STOPPED\`"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$STOPPED\`.src (x Int64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$STOPPED\`.src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW \`$STOPPED\`.mv REFRESH EVERY 1 SECOND
    (x Int64) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT x FROM \`$STOPPED\`.src"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP VIEW \`$STOPPED\`.mv"
# The view refreshes every second, and `SYSTEM STOP VIEW` interrupts a
# running refresh without waiting for it to unwind. Wait until the view is idle, so that the
# `EXCHANGE` and `DROP` of its target cannot race the backup scan and warn on stderr.
while [ "$(${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.view_refreshes WHERE database = '$STOPPED'")" != 'Disabled' ]
do
    sleep 0.1
done
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE \`$STOPPED\` TO Disk('backups', '$BACKUP_STOPPED')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`$STOPPED\` SYNC"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT restore_pause_before_data_restore_tasks"

# Pauses after the tables are created but before the restore is finalized, which is the window where
# the view exists and is held back. Its output is discarded because it interleaves with the echoes.
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE \`$STOPPED\` FROM Disk('backups', '$BACKUP_STOPPED')" > /dev/null &
RESTORE_PID=$!
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT restore_pause_before_data_restore_tasks PAUSE"

echo "1. held back while the restore is still running:"
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.view_refreshes WHERE database = '$STOPPED' FORMAT TSV"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP VIEW \`$STOPPED\`.mv"
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT restore_pause_before_data_restore_tasks"
wait $RESTORE_PID

# The view refreshes every second, so this is long enough for a released one to leave Disabled and
# write to its target.
sleep 2

echo "2. the stop survives the finished restore, target untouched:"
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT status FROM system.view_refreshes WHERE database = '$STOPPED'),
    (SELECT count() FROM \`$STOPPED\`.mv) FORMAT TSV"

${CLICKHOUSE_CLIENT} -q "SYSTEM START VIEW \`$STOPPED\`.mv"
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT VIEW \`$STOPPED\`.mv"
echo "3. and it refreshes once actually started:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`$STOPPED\`.mv FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP VIEW \`$STOPPED\`.mv"
