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

MT_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_source_access'")

echo "=== static structures stay readable with no grants ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none') FORMAT TSV" | cut -f 1
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeAnalyzeIndexes(currentDatabase(), t_source_access) FORMAT TSV" | cut -f 1

echo "=== the UUID form is unchanged by this fix ==="
as_user "SELECT count() FROM mergeTreeAnalyzeIndexesUUID('$MT_UUID')"

echo "=== viewIfPermitted delegation is a no-op for a function with no source ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT * FROM viewIfPermitted(SELECT 1 AS x ELSE null('x UInt8'))"
# `executable` is the only ELSE engine that requires a `TABLE ENGINE` grant, which resolving its
# structure owes as well. Printing the type instead means the grant was not required.
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE viewIfPermitted(SELECT 1 AS x ELSE executable('x.sh', 'TSV', 'x UInt8'))" 2>&1 | grep -o "ACCESS_DENIED\|UInt8" | head -1
# The delegated check must also be satisfiable: with the grant the same DESCRIBE resolves.
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON Executable TO $user_name"
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE viewIfPermitted(SELECT 1 AS x ELSE executable('x.sh', 'TSV', 'x UInt8'))" 2>&1 | grep -o "ACCESS_DENIED\|UInt8" | head -1
$CLICKHOUSE_CLIENT -q "REVOKE TABLE ENGINE ON Executable FROM $user_name"
# The primary query must be one this user cannot run, or viewIfPermitted answers with it instead
# of the ELSE function, so it reads a table this user can see but not read.
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM viewIfPermitted(SELECT * FROM t_alias_src ELSE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none'))" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq

echo "=== the column mismatch diagnostic is unchanged (default user) ==="
$CLICKHOUSE_CLIENT -q "SELECT count() FROM viewIfPermitted(SELECT 1 AS x ELSE null('y UInt8'))" 2>&1 | grep -o "requires a SELECT query with the result columns matching a table function after 'ELSE'" | uniq

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_source_access;
DROP USER IF EXISTS $user_name;
"
