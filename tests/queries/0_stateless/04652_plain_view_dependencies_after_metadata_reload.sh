#!/usr/bin/env bash
# Regression test for the dependencies of a plain view rebuilt from metadata.
#
# A plain (non-materialized) view on an unqualified source feeds two graphs in
# `DatabaseCatalog`, and both are rebuilt when the metadata is reloaded:
#
#   * `plain_view_dependencies`, reported by `system.tables.dependencies_*`, rebuilt by
#     `DatabaseOrdinary::loadTableFromMetadataAsync`;
#   * `referential_dependencies`, which keeps a source table from being dropped out from
#     under the view, rebuilt by `TablesLoader::buildDependencyGraph`.
#
# In a live session both are filled by `InterpreterCreateQuery` against the session's
# current database. The reload instead runs with the shared `TablesLoader` context, whose
# current database is the server default (`default`), so both must resolve the view's
# source tables against the database that owns the view and not against that context.
# Resolving against the context silently moves the view onto a same-named table in
# another database.
#
# `DETACH DATABASE` / `ATTACH DATABASE` goes through the very same `TablesLoader` path as
# startup, so it reproduces the restart without restarting the server. `ATTACH DATABASE`
# waits for the load and startup tasks before returning, so the assertions below do not
# race with the async per-table load jobs.
#
# Only half of the misresolution is observable from a stateless test: that the view's own
# source loses its protection. The other half - an unrelated table in `default` gaining a
# phantom dependent - would need a table in `default`, which is shared with every other
# test running in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# A dedicated non-default database, so it can be detached and attached without touching
# the database the test harness created for this run (which is used as the "other"
# database for the cross-database view below).
DB="${CLICKHOUSE_DATABASE}_views"

trap '$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null || true' EXIT

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64) ENGINE = MergeTree ORDER BY id"

# Everything below runs in one session whose current database is ${DB}.
#
# The temporary table shadowing `src` is what makes this test exercise the load path:
# `AddDefaultDatabaseVisitor` (called from `InterpreterCreateQuery` for views) qualifies
# unqualified sources with the current database, except for session-local external
# tables. With a temporary `src` in the session the stored definition of `v` therefore
# keeps the bare `FROM src`, which is the shape whose resolution on the metadata loading
# path used to be wrong. Do not remove the temporary table - without it the stored
# definition is already qualified and the reload below checks nothing.
$CLICKHOUSE_CLIENT -q "
USE ${DB};
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TEMPORARY TABLE src (id UInt64) ENGINE = Memory;
CREATE VIEW v AS SELECT * FROM src;
CREATE VIEW v_xdb AS SELECT * FROM ${CLICKHOUSE_DATABASE}.src;
"

# Qualified on purpose: in the session above `src` would resolve to the temporary table.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.src VALUES (42)"

# Guard the precondition: the source of `v` must be stored unqualified.
echo '--- stored definition of the view ---'
$CLICKHOUSE_CLIENT -q "
SELECT replaceOne(create_table_query, '${DB}', 'views_db')
FROM system.tables WHERE database = '${DB}' AND name = 'v' FORMAT TSVRaw"

# Dependents of every table named `src`, with the random database names replaced by
# stable labels so the reference file is deterministic.
show_dependents()
{
    $CLICKHOUSE_CLIENT -q "
    SELECT
        multiIf(database = '${DB}', 'views_db', database = currentDatabase(), 'other_db', database) AS db,
        name,
        arraySort(arrayMap((d, t) -> concat(multiIf(d = '${DB}', 'views_db', d = currentDatabase(), 'other_db', d), '.', t),
                           dependencies_database, dependencies_table)) AS dependents
    FROM system.tables
    WHERE name = 'src' AND database IN ('${DB}', currentDatabase())
    ORDER BY db"
}

echo '--- dependents before the reload ---'
show_dependents

# Reload the metadata of ${DB} from a session that has no temporary `src` and whose
# current database is not ${DB} - exactly the situation of a server restart.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB}"

# `v` must be reported as a dependent of `${DB}.src` (the source in the view's own
# database), and never of the same-named table in another database.
echo '--- dependents after the reload ---'
show_dependents

# The registered dependency must agree with the table the reloaded view reads when it is
# queried from its own database (42 comes from ${DB}.src, the other `src` is empty).
echo '--- the reloaded view reads the source it depends on ---'
$CLICKHOUSE_CLIENT -q "USE ${DB}; SELECT * FROM v"

# The referential dependency graph is rebuilt on the same metadata loading path, and it is
# what protects a source table from being dropped out from under a view. Resolving the
# unqualified source against the loading context instead of the view's own database moves
# that protection to a same-named table in another database: `${DB}.src` becomes droppable
# while an unrelated table keeps a phantom dependent.
#
# `check_referential_table_dependencies` is off by default, so ask for the check explicitly.
# The whole client output is captured, so a failing DROP does not leak to stderr.
drop_is_blocked()
{
    local output
    output=$($CLICKHOUSE_CLIENT --check_referential_table_dependencies=1 -q "DROP TABLE $1" 2>&1 || true)
    case "$output" in
        *HAVE_DEPENDENT_OBJECTS*) echo 'blocked' ;;
        '')                       echo 'DROPPED' ;;
        *)                        echo "unexpected: $output" ;;
    esac
}

echo '--- the source of the reloaded view is protected from DROP ---'
echo -n 'views_db.src (unqualified source of views_db.v): '
drop_is_blocked "${DB}.src"
echo -n 'other_db.src (qualified source of views_db.v_xdb): '
drop_is_blocked "${CLICKHOUSE_DATABASE}.src"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB} SYNC"
