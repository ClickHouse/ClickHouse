#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: the PAUSEABLE_ONCE failpoint fires exactly once globally, so a `RENAME DATABASE` from
#   another parallel test could take the pause meant for this test's rename.
# no-replicated-database: failpoints are single-server, and table DDL inside a Replicated database
#   goes through the DDL queue instead of taking the database DDL lock directly.

# https://github.com/ClickHouse/ClickHouse/issues/116906
# The dependency check of `RENAME DATABASE` must cover exactly the tables that get renamed. Without
# the exclusive database DDL lock, a `CREATE TABLE db.t ENGINE = Alias(<new name>, 't')` racing with
# the rename could be committed after the check and before the catalog rewrite, and the rename would
# then move the never-checked table and leave `new.t = Alias(new, t)`: a cycle in the server-wide
# dependency graph that fails every later `CREATE` adding a dependency edge with `INFINITE_LOOP`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
FAILPOINT="rename_database_after_dependency_check"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_from" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_to" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB}_from;
    CREATE TABLE ${DB}_from.harmless (x UInt8) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

# The rename passes the dependency check (the database has no dependencies) and pauses right before
# the catalog is rewritten, holding the exclusive DDL lock of the source database.
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${DB}_from TO ${DB}_to" &
RENAME_PID=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

# A table that would become a self-reference after the rename. It must not be able to slip into the
# database while the rename is in flight: table-level DDL takes the shared side of the database DDL
# lock, which the rename holds exclusively, so it fails instead of waiting behind a paused rename.
# Without that lock the CREATE would first register its dependency edge and then block on the
# database mutex until the rename resumes, so bound it instead of letting the test hang.
timeout 60 ${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}_from.t ENGINE = Alias('${DB}_to', 't')" 2>/dev/null
case $? in
    0) echo "CREATE TABLE succeeded during RENAME DATABASE" ;;
    124) echo "CREATE TABLE still blocked while RENAME DATABASE is paused" ;;
    *) echo "CREATE TABLE rejected during RENAME DATABASE" ;;
esac

${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT ${FAILPOINT}"
wait ${RENAME_PID}

# The rename went through with the checked table set only, and the dependency graph is intact:
# unrelated DDL that adds a dependency edge still works.
${CLICKHOUSE_CLIENT} -nm -q "
    SELECT name FROM system.tables WHERE database = '${DB}_to' ORDER BY name;
    CREATE VIEW v AS SELECT x FROM ${DB}_to.harmless;
    SELECT count() FROM v;
    DROP VIEW v;
    DROP DATABASE ${DB}_to;
"
