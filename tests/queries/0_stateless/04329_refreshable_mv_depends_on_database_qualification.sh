#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for issue #106737 (STID 2508-3e7b):
#   Logical error 'Unexpected exception in refresh scheduling'.
#
# `REFRESH ... DEPENDS ON <table>` dependencies were turned into StorageIDs straight from the
# AST. When a bare dependency name matched a TEMPORARY table (or CTE alias),
# AddDefaultDatabaseVisitor leaves it unqualified, so the StorageID had an empty database. On the
# first scheduling pass StorageID::getFullTableName() threw UNKNOWN_DATABASE, the catch-all in
# RefreshTask::doScheduling re-raised it as a LOGICAL_ERROR and the server aborted (then
# crash-looped on restart, because the bad definition was persisted).
#
# The fix resolves any still-unqualified dependency against the view's database in
# RefreshTask::create / alterRefreshParams (which also covers the attach-from-metadata path that
# has no session). The state assertion below catches the bug even in non-sanitizer builds, where
# the catch-all does not abort: with the empty-database bug the scheduler records the exception
# and stops (Disabled with a non-empty exception) instead of settling at MissingDependencies.

DB2="${CLICKHOUSE_DATABASE}_mv"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DB2\` SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$DB2\`"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$DB2\`.t (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$DB2\`.dst1 (a UInt64) ENGINE = MergeTree ORDER BY a"

# Poll until the view's refresh has settled (left the transient Scheduling state), then print the
# settled status and whether the exception is empty. The dependency `t` resolves to a plain table
# (never a refreshable MV), so the scheduler settles permanently at MissingDependencies with no
# exception. With the original empty-database bug it settles at Disabled with a non-empty
# exception instead (or, in debug/sanitizer builds, the server aborts before reaching here).
report_refresh_state() {
    local view="$1"
    local status=""
    local i
    for i in $(seq 1 120); do
        status=$(${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.view_refreshes WHERE database = '$DB2' AND view = '$view'")
        if [ -n "$status" ] && [ "$status" != "Scheduling" ]; then
            break
        fi
        sleep 0.5
    done
    ${CLICKHOUSE_CLIENT} -q "
        SELECT status, exception = '' AS no_exception
        FROM system.view_refreshes WHERE database = '$DB2' AND view = '$view' FORMAT TSV"
}

# 1. A temporary table shadowing the dependency name must not crash the refresh scheduler.
#    The temporary table and the MV must be created in the SAME client session, so the temporary
#    table is visible while the CREATE is interpreted and AddDefaultDatabaseVisitor leaves the
#    bare `t` unqualified. The dependency must not be left with an empty database (which crashed
#    scheduling); it must settle at MissingDependencies.
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE TEMPORARY TABLE t (a UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW \`$DB2\`.mv_tmp REFRESH EVERY 1 SECOND DEPENDS ON t
        TO \`$DB2\`.dst1 AS SELECT a FROM \`$DB2\`.t;
"
echo "1. temporary-table-named dependency, settled status and empty exception:"
report_refresh_state mv_tmp

# 2. The persisted definition must survive a server restart without crash-looping. Detaching and
#    re-attaching the view replays the attach-from-metadata code path (no session), where the
#    empty-database dependency previously aborted the server on the first scheduling pass.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE \`$DB2\`.mv_tmp"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE \`$DB2\`.mv_tmp"
echo "2. same view after detach/attach (attach-from-metadata path), settled status and empty exception:"
report_refresh_state mv_tmp

# 3. The server must still be alive (it would have aborted without the fix in debug/sanitizer builds).
echo "3. server alive:"
${CLICKHOUSE_CLIENT} -q "SELECT 1"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`$DB2\` SYNC"
