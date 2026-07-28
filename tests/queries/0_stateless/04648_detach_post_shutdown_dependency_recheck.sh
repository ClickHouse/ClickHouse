#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: relies on a failpoint (libfiu), which the fast-test build does not include.
# no-parallel: the failpoint sits on the generic DROP/DETACH path, so while it is enabled any
#              concurrent DROP from another test in the shared server would consume the pause.
# no-replicated-database: in a Replicated database InterpreterDropQuery::executeToTableImpl returns
#              early through tryEnqueueReplicatedDDL and never reaches the failpoint, so
#              `SYSTEM WAIT FAILPOINT ... PAUSE` would block until the test times out.
#
# A dependent registered concurrently (after the pre-shutdown dependency check passed) used to make
# DROP / DETACH ... PERMANENTLY throw HAVE_DEPENDENT_OBJECTS *after* the object was already shut down,
# leaving it attached but broken -- a dictionary unloadable, a MaterializedView silently no longer
# receiving inserts. The removal must now finish instead, and log a warning naming the dependents.
#
# The failpoint pauses the removal between shutdown and dependency removal so the dependent can be
# created deterministically in that window.
#
# NOTE: the dependent must reference the object by a *qualified* name. An unqualified dictionary name
# is resolved through the dictionaries loader, which no longer knows the dictionary once
# flushAndShutdown has deregistered it, so no dependency edge would be recorded and the race would
# not be exercised at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=detach_permanently_pause_before_remove_dependencies
DB=${CLICKHOUSE_DATABASE}

# Always release the failpoint, otherwise a failed assertion would leave a server thread parked.
cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# Runs $2 (a removal) while $3 (a query registering a dependent on it) executes in the paused window.
race_removal() {
    local label=$1 removal=$2 registrant=$3 removal_opts=${4:-}
    # A unique query id per removal: system.text_log is shared by every test on this server, so the
    # warning assertions must not be satisfiable by a row from an earlier run or an earlier scenario.
    QUERY_ID="${CLICKHOUSE_DATABASE}_${label}_$$"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $removal_opts --query_id "$QUERY_ID" --query "$removal" > /dev/null 2>"${CLICKHOUSE_TMP}/${label}.err" &
    local removal_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    $CLICKHOUSE_CLIENT --query "$registrant"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

    if wait "$removal_pid"; then
        echo "$label removal=ok"
    else
        echo "$label removal=FAILED $(grep -o 'Code: [0-9]*' "${CLICKHOUSE_TMP}/${label}.err" | head -1)"
    fi
    rm -f "${CLICKHOUSE_TMP}/${label}.err"
}

# Prints 1 if the "left referencing a removed object" warning was logged by the last removal for $1
# (a qualified name), else 0. Filtered by the removal's query id so stale rows cannot satisfy it.
warned_for() {
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT count() > 0
        FROM system.text_log
        WHERE query_id = '${QUERY_ID}'
          AND level = 'Warning'
          AND message LIKE 'Removing $1 %still depend on it%were registered concurrently%'
    "
}

echo "-- dictionary"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE src (id UInt64, val String) ENGINE = Memory;
INSERT INTO src VALUES (1, 'a');
CREATE DICTIONARY dict (id UInt64, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB '${DB}'))
LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
SELECT dictGetString('${DB}.dict', 'val', 1);
"
race_removal dict \
    "DETACH DICTIONARY dict PERMANENTLY" \
    "CREATE TABLE dep_dict (id UInt64, v String DEFAULT dictGetString('${DB}.dict', 'val', id)) ENGINE = Memory"
# Cleanly removed, not left 'attached but unloadable'.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict'"
echo "warned=$(warned_for ${DB}.dict)"

echo "-- materialized view"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE mv_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id AS SELECT id, val FROM mv_src;
INSERT INTO mv_src VALUES (1, 'x');
SELECT count() FROM mv;
"
race_removal mv \
    "DETACH TABLE mv PERMANENTLY" \
    "CREATE DICTIONARY dep_mv (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'mv' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv'"
echo "warned=$(warned_for ${DB}.mv)"
# The source keeps working: no silently half-detached view left behind.
$CLICKHOUSE_CLIENT --query "INSERT INTO mv_src VALUES (2, 'y')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM mv_src"

echo "-- plain MergeTree, DROP instead of DETACH"
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
race_removal mt \
    "DROP TABLE mt" \
    "CREATE DICTIONARY dep_mt (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'mt' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mt'"
echo "warned=$(warned_for ${DB}.mt)"

echo "-- the surviving edge is real: a new object under the same name is protected again"
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "DROP TABLE mt" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1

echo "-- and dropping the dependent clears it"
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY dep_mt"
$CLICKHOUSE_CLIENT --query "DROP TABLE mt"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mt'"

echo "-- ReplicatedMergeTree (in an Atomic database)"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE rmt (id UInt64, val String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rmt', 'r1')
ORDER BY id;
INSERT INTO rmt VALUES (1, 'z');
"
race_removal rmt \
    "DETACH TABLE rmt PERMANENTLY" \
    "CREATE DICTIONARY dep_rmt (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'rmt' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'rmt'"
echo "warned=$(warned_for ${DB}.rmt)"

# check_referential_table_dependencies defaults to 0, so every scenario above consults the *loading*
# dependency graph. getBlockingDependentsUnlocked has a second branch for the referential graph; turning
# the setting on for the removal exercises it.
echo "-- referential dependency check (check_referential_table_dependencies = 1)"
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt_ref (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
race_removal mt_ref \
    "DROP TABLE mt_ref" \
    "CREATE DICTIONARY dep_mt_ref (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'mt_ref' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)" \
    "--check_referential_table_dependencies 1"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mt_ref'"
echo "warned=$(warned_for ${DB}.mt_ref)"

# DROP DATABASE keeps rejecting only *cross-database* dependents: same-database ones are filtered out of
# the blocking set (DatabaseCatalog::getBlockingDependentsUnlocked with is_drop_database), so they must not
# produce the warning either. A non-failpoint version of this cannot test the filter: DROP DATABASE drops
# dependents before their dependencies, so by the time the victim reaches the policy decision both the
# filtered and the unfiltered set are already empty.

echo "-- DROP DATABASE, dependent registered in the window in the SAME database: filtered, no warning"
DB_N1=${DB}_n1
$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS ${DB_N1};
CREATE DATABASE ${DB_N1};
CREATE TABLE ${DB_N1}.vic (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
"
race_removal same_db \
    "DROP DATABASE ${DB_N1}" \
    "CREATE DICTIONARY ${DB_N1}.dep_same (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'vic' DB '${DB_N1}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
echo "warned=$(warned_for ${DB_N1}.vic)"

echo "-- DROP DATABASE, dependent registered in the window in ANOTHER database: warned"
DB_N2=${DB}_n2
$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS ${DB_N2};
CREATE DATABASE ${DB_N2};
CREATE TABLE ${DB_N2}.vic (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
"
race_removal cross_db \
    "DROP DATABASE ${DB_N2}" \
    "CREATE DICTIONARY dep_cross (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'vic' DB '${DB_N2}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
echo "warned=$(warned_for ${DB_N2}.vic)"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS dep_dict;
DROP DICTIONARY IF EXISTS dep_mv;
DROP DICTIONARY IF EXISTS dep_rmt;
DROP DICTIONARY IF EXISTS dep_mt_ref;
DROP TABLE IF EXISTS mv_src;
DROP TABLE IF EXISTS src;
DROP DICTIONARY IF EXISTS dep_cross;
"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_N1}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_N2}"
