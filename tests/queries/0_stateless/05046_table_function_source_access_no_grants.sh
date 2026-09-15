#!/usr/bin/env bash
# Tags: no-replicated-database
# In a Replicated database a CREATE TABLE ... AS <table function> is carried out by the DDL worker,
# which runs with no user and therefore full access, so a check at the execution seam cannot deny.

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
DROP TABLE IF EXISTS t_ok_as_tf_p;
DROP TABLE IF EXISTS t_ok_as_tf_ai;
DROP TABLE IF EXISTS t_ok_as_tf_ti;
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

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_as_tf;
DROP TABLE IF EXISTS t_as_tf_p;
DROP TABLE IF EXISTS t_as_tf_ti;
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_source_access;
DROP USER IF EXISTS $user_name;
"
