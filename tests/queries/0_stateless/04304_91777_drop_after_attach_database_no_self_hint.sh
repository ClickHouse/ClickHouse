#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91777
# After server restart (or DETACH/ATTACH DATABASE) an operation on an async-loaded
# table used to leave a stale entry in the `startup_table` map, so a later lookup
# of a similar name produced a confusing hint pointing at a table that no longer
# exists: "Table X.Y does not exist. Maybe you meant X.Z?".
# Two paths populate/leak that map:
#   1. DROP TABLE   - cleared via DatabaseOrdinary::detachTableUnlocked.
#   2. RENAME TABLE - DatabaseAtomic::renameTable detaches via a local lambda that
#                     bypasses detachTableUnlocked, so the old name stayed in
#                     startup_table and getAllTableNames kept suggesting it.
# Both scenarios look up a name one edit away from the removed table so the bug
# reproduces independently of the exact-name self-suggestion already suppressed
# by getHintForTable (the hint must never point back at a removed name).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fail fast on any unexpected setup failure (the only tolerated failures are the
# expected typo lookups below). Set after sourcing shell_config.sh so its internals
# are not affected.
set -euo pipefail

DB="db_91777_${CLICKHOUSE_DATABASE}"

# Always clean up the test database, even if a setup command fails.
trap '$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null || true' EXIT

# --- Scenario 1: DROP TABLE leaves no stale hint for the dropped name --------
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.tbl0 (c Int) ENGINE = MergeTree ORDER BY tuple()"

# Re-trigger the async-load path that populates startup_table.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB}"

# Drop the async-loaded table. Without the fix it stays in startup_table.
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.tbl0 SYNC"

# Look up a name one edit away from the dropped `tbl0` (and far from anything else).
# If the stale `tbl0` lingered it would be offered as a hint even though the table
# no longer exists. The hint is database-qualified, so a same-named table in a
# concurrent parallel test's database does not match this check.
err=$($CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.tbl1 SYNC" 2>&1 || true)
if echo "${err}" | grep -qF "Maybe you meant ${DB}.tbl0?"; then
    echo "BAD: stale startup_table entry leaked into hint after DROP"
    echo "${err}"
else
    echo "OK"
fi

# --- Scenario 2: RENAME TABLE leaves no stale hint for the old name ----------
# Recreate the database so the async-load map starts clean, then rename an
# async-loaded table and verify the old name is gone from the hint set.
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.alpha (c Int) ENGINE = MergeTree ORDER BY tuple()"

# Re-trigger the async-load path so startup_table holds {alpha}.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB}"

# Rename the async-loaded table. The renamed-away name must not survive in
# startup_table; otherwise getAllTableNames keeps offering it as a hint.
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${DB}.alpha TO ${DB}.beta"

# Look up a name one edit away from the old `alpha` (and far from `beta`). If the
# stale `alpha` lingered it would be suggested even though the table no longer
# exists; with the leak fixed only `beta` exists and is too distant to match.
err=$($CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.alphx SYNC" 2>&1 || true)
if echo "${err}" | grep -qF "Maybe you meant ${DB}.alpha?"; then
    echo "BAD: stale startup_table entry leaked into hint after RENAME"
    echo "${err}"
else
    echo "OK"
fi
