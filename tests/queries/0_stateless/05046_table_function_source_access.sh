#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_05046"

as_user()
{
    $CLICKHOUSE_CLIENT --user "$user_name" -q "$1" 2>&1 | grep -o "ACCESS_DENIED" | uniq
}

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_source_access;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_as_tf;
DROP TABLE IF EXISTS t_as_tf_p;
DROP TABLE IF EXISTS t_as_tf_ti;
DROP USER IF EXISTS $user_name;

CREATE TABLE t_source_access (a UInt64, b UInt64, PROJECTION p_src (SELECT b ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_source_access SELECT number, number FROM numbers(100);

CREATE TABLE t_not_mergetree (a UInt64) ENGINE = Log;
CREATE TABLE t_alias_src ENGINE = Alias(currentDatabase(), 't_source_access');

CREATE USER $user_name NOT IDENTIFIED;

-- The exact table only. A database-wide CREATE TABLE grant would also confer SHOW TABLES on the
-- source tables and every arm below would pass the check it is meant to exercise.
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_as_tf TO $user_name;
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_as_tf_p TO $user_name;
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_as_tf_ti TO $user_name;
GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.t_alias_src TO $user_name;
"

MT_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_source_access'")

echo "=== arming: the user cannot see either source table ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_source_access'"
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_not_mergetree'"

echo "=== structure, no grants ==="
as_user "DESCRIBE t_source_access"
as_user "DESCRIBE mergeTreeIndex(currentDatabase(), t_source_access)"
as_user "DESCRIBE mergeTreeIndex(currentDatabase(), t_source_access, with_marks = true)"
as_user "DESCRIBE mergeTreeProjection(currentDatabase(), t_source_access, p_src)"
as_user "DESCRIBE mergeTreeIndex(currentDatabase(), t_not_mergetree)"
as_user "DESCRIBE mergeTreeProjection(currentDatabase(), t_not_mergetree, p_src)"
as_user "DESCRIBE viewIfPermitted(SELECT 1 ELSE mergeTreeIndex(currentDatabase(), t_source_access))"

echo "=== select, no grants ==="
as_user "SELECT count() FROM mergeTreeIndex(currentDatabase(), t_not_mergetree)"
as_user "SELECT count() FROM mergeTreeProjection(currentDatabase(), t_not_mergetree, p_src)"
as_user "SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_not_mergetree, 'idx_none')"
as_user "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_no_such_table_here)"
as_user "SELECT count() FROM viewIfPermitted(SELECT 1 ELSE mergeTreeIndex(currentDatabase(), t_source_access))"
as_user "DESCRIBE viewIfPermitted(SELECT 1 ELSE url('http://localhost:1/', TSV, 'x UInt8'))"

echo "=== alias, SHOW COLUMNS on the alias but not on its target ==="
as_user "DESCRIBE t_alias_src"
as_user "DESCRIBE mergeTreeProjection(currentDatabase(), t_alias_src, p_src)"
# Reading has its own check site, and without one the caller reaches an internal cast failure.
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM mergeTreeProjection(currentDatabase(), t_alias_src, p_src)" 2>&1 | grep -o "ACCESS_DENIED\|std::bad_cast" | uniq
as_user "DESCRIBE loop(currentDatabase(), t_alias_src)"
# An index name is metadata too, so the denial must come before the index lookup reports it missing.
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_alias_src, 'idx_none')" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq
# CREATE TABLE ... AS supplies the columns, so the storage is built lazily under the global full-access
# context: the denial has to come from the seam, which still runs under this user.
as_user "CREATE TABLE t_as_tf_p (b UInt64) AS mergeTreeProjection(currentDatabase(), t_alias_src, p_src)"
as_user "CREATE TABLE t_as_tf_ti AS mergeTreeTextIndex(currentDatabase(), t_alias_src, 'idx_none')"

echo "=== create as table function, no grants on the source ==="
as_user "CREATE TABLE t_as_tf AS mergeTreeIndex(currentDatabase(), t_source_access)"

echo "=== static structures stay readable with no grants ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none') FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeAnalyzeIndexes(currentDatabase(), t_source_access) FORMAT TSV" | cut -f 1

echo "=== the UUID form is unchanged by this fix ==="
as_user "SELECT count() FROM mergeTreeAnalyzeIndexesUUID('$MT_UUID')"

echo "=== viewIfPermitted delegation is a no-op for a function with no source ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT * FROM viewIfPermitted(SELECT 1 AS x ELSE null('x UInt8'))"
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM viewIfPermitted(SELECT 1 ELSE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none'))" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq

echo "=== the column mismatch diagnostic is unchanged (default user) ==="
$CLICKHOUSE_CLIENT -q "SELECT count() FROM viewIfPermitted(SELECT 1 AS x ELSE null('y UInt8'))" 2>&1 | grep -o "requires a SELECT query with the result columns matching a table function after 'ELSE'" | uniq

# A second user that can see the table but not describe it. viewIfPermitted resolves the ELSE
# function's structure and prints it in its column mismatch error, so it must require what
# describing that source requires, not merely what naming it requires.
user_show_tables="${CLICKHOUSE_DATABASE}_test_user_05046_st"
$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_show_tables;
CREATE USER $user_show_tables NOT IDENTIFIED;
GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE.t_source_access TO $user_show_tables;
"

echo "=== SHOW TABLES but not SHOW COLUMNS ==="
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_source_access'"
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "DESCRIBE t_source_access" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "SELECT count() FROM viewIfPermitted(SELECT 1 ELSE mergeTreeIndex(currentDatabase(), t_source_access))" 2>&1 | grep -o "ACCESS_DENIED\|the table function after 'ELSE' gives" | uniq

# A third user holding exactly what a DESCRIBE of the source table requires and nothing more, so the
# arms below fail if the structure seam demands anything stronger than SHOW COLUMNS.
user_show_columns="${CLICKHOUSE_DATABASE}_test_user_05046_sc"
$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_show_columns;
CREATE USER $user_show_columns NOT IDENTIFIED;
GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.t_source_access TO $user_show_columns;
"

echo "=== SHOW COLUMNS only ==="
$CLICKHOUSE_CLIENT --user "$user_show_columns" -q "DESCRIBE t_source_access FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_show_columns" -q "DESCRIBE mergeTreeIndex(currentDatabase(), t_source_access) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_show_columns" -q "DESCRIBE mergeTreeProjection(currentDatabase(), t_source_access, p_src) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_show_columns" -q "SELECT a FROM mergeTreeIndex(currentDatabase(), t_source_access)" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_show_columns" -q "SELECT b FROM mergeTreeProjection(currentDatabase(), t_source_access, p_src)" 2>&1 | grep -o "ACCESS_DENIED" | uniq

# A fourth user holding SHOW COLUMNS on the alias and on its target and nothing else, so the arms
# below fail if the alias leg demands anything stronger than SHOW COLUMNS on the target.
user_alias_both="${CLICKHOUSE_DATABASE}_test_user_05046_ab"
$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_alias_both;
CREATE USER $user_alias_both NOT IDENTIFIED;
GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.t_alias_src TO $user_alias_both;
GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.t_source_access TO $user_alias_both;
"

echo "=== SHOW COLUMNS on alias and target ==="
$CLICKHOUSE_CLIENT --user "$user_alias_both" -q "DESCRIBE mergeTreeProjection(currentDatabase(), t_alias_src, p_src) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_alias_both" -q "DESCRIBE loop(currentDatabase(), t_alias_src) FORMAT TSV" | cut -f 1

$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON $CLICKHOUSE_DATABASE.t_source_access TO $user_name"

echo "=== column-only grant ==="
as_user "DESCRIBE t_source_access"
as_user "DESCRIBE mergeTreeIndex(currentDatabase(), t_source_access)"
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT a FROM mergeTreeIndex(currentDatabase(), t_source_access) ORDER BY a LIMIT 1"
as_user "SELECT count() FROM mergeTreeProjection(currentDatabase(), t_source_access, p_src)"

$CLICKHOUSE_CLIENT -q "
REVOKE SELECT ON $CLICKHOUSE_DATABASE.t_source_access FROM $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.t_source_access TO $user_name;
"

echo "=== table-level grant ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE t_source_access FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeIndex(currentDatabase(), t_source_access) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeProjection(currentDatabase(), t_source_access, p_src) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE loop(currentDatabase(), t_source_access) FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() > 0 FROM mergeTreeIndex(currentDatabase(), t_source_access)"
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() > 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_source_access)"
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none')" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq
# The delegated check must also be satisfiable: a granted caller reaches the index lookup.
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM viewIfPermitted(SELECT 1 ELSE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none'))" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq

echo "=== an authorized user still gets the original error for a source that does not exist ==="
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS, SELECT ON $CLICKHOUSE_DATABASE.t_missing TO $user_name"
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeIndex(currentDatabase(), t_missing)" 2>&1 | grep -o "UNKNOWN_TABLE\|ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_missing)" 2>&1 | grep -o "UNKNOWN_TABLE\|ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_as_tf;
DROP TABLE IF EXISTS t_as_tf_p;
DROP TABLE IF EXISTS t_as_tf_ti;
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_source_access;
DROP USER IF EXISTS $user_name;
DROP USER IF EXISTS $user_show_tables;
DROP USER IF EXISTS $user_show_columns;
DROP USER IF EXISTS $user_alias_both;
"
