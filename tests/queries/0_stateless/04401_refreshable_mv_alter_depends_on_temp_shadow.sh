#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Exercises ALTER ... MODIFY REFRESH ... DEPENDS ON where the bare dependency name is shadowed by a
# TEMPORARY table, so the StorageID reaches alterRefreshParams with an empty database. The view must
# settle at MissingDependencies with an empty exception, not abort scheduling.

DB2="${CLICKHOUSE_DATABASE}_mv"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DB2\` SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$DB2\`"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$DB2\`.t (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$DB2\`.dst1 (a UInt64) ENGINE = MergeTree ORDER BY a"

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

# Temp table and ALTER must run in the SAME session so AddDefaultDatabaseVisitor leaves bare `t` unqualified.
# Create with a qualified DEPENDS ON so the view cannot run before the ALTER (first timeslot is otherwise
# immediately due and the scheduler could flip to Running, masking the expected MissingDependencies status).
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE TEMPORARY TABLE t (a UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW \`$DB2\`.mv_alt REFRESH EVERY 1 SECOND DEPENDS ON \`$DB2\`.t TO \`$DB2\`.dst1 AS SELECT a FROM \`$DB2\`.t;
    ALTER TABLE \`$DB2\`.mv_alt MODIFY REFRESH EVERY 1 SECOND DEPENDS ON t;
"
echo "1. ALTER MODIFY REFRESH DEPENDS ON temp-shadowed name, settled status and empty exception:"
report_refresh_state mv_alt

echo "2. server alive:"
${CLICKHOUSE_CLIENT} -q "SELECT 1"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`$DB2\` SYNC"
