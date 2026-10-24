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
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM viewIfPermitted(SELECT * FROM t_alias_src ELSE mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none'))" 2>&1 | grep -o "ACCESS_DENIED\|There is no index with name 'idx_none'" | uniq

echo "=== an authorized user still gets the original error for a source that does not exist ==="
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS, SELECT ON $CLICKHOUSE_DATABASE.t_missing TO $user_name"
$CLICKHOUSE_CLIENT --user "$user_name" -q "DESCRIBE mergeTreeIndex(currentDatabase(), t_missing)" 2>&1 | grep -o "UNKNOWN_TABLE\|ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_missing)" 2>&1 | grep -o "UNKNOWN_TABLE\|ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_source_access;
DROP USER IF EXISTS $user_name;
"
