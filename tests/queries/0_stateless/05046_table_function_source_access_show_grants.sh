#!/usr/bin/env bash
# Tags: no-replicated-database
# In a Replicated database a CREATE TABLE ... AS <table function> is carried out by the DDL worker,
# which runs with no user and therefore full access, so a check at the execution seam cannot deny.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_05046"

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

# A second user that can see the table but not describe it. viewIfPermitted resolves the ELSE
# function's structure and prints it in its column mismatch error, so it must require what
# describing that source requires, not merely what naming it requires.
user_show_tables="${CLICKHOUSE_DATABASE}_test_user_05046_st"
$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_show_tables;
CREATE USER $user_show_tables NOT IDENTIFIED;
GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE.t_source_access TO $user_show_tables;
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_ok_as_tf_p TO $user_show_tables;
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_ok_as_tf_ai TO $user_show_tables;
GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.t_ok_as_tf_ti TO $user_show_tables;
"

echo "=== SHOW TABLES but not SHOW COLUMNS ==="
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_source_access'"
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "DESCRIBE t_source_access" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "SELECT count() FROM viewIfPermitted(SELECT 1 ELSE mergeTreeIndex(currentDatabase(), t_source_access))" 2>&1 | grep -o "ACCESS_DENIED\|the table function after 'ELSE' gives" | uniq
# Naming the source is all the execution seam asks of these three, so exactly `SHOW TABLES` must
# suffice. Explicit columns keep the storage lazy, so the seam is the only thing that runs, and
# the count is 3 unless the seam demands something stronger.
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "CREATE TABLE t_ok_as_tf_p (b UInt64) AS mergeTreeProjection(currentDatabase(), t_source_access, p_src)" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "CREATE TABLE t_ok_as_tf_ai (part_name String) AS mergeTreeAnalyzeIndexes(currentDatabase(), t_source_access)" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT --user "$user_show_tables" -q "CREATE TABLE t_ok_as_tf_ti (part_name String) AS mergeTreeTextIndex(currentDatabase(), t_source_access, 'idx_none')" 2>&1 | grep -o "ACCESS_DENIED" | uniq
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_ok_as_tf_p', 't_ok_as_tf_ai', 't_ok_as_tf_ti')"

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

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_ok_as_tf_p;
DROP TABLE IF EXISTS t_ok_as_tf_ai;
DROP TABLE IF EXISTS t_ok_as_tf_ti;
DROP TABLE IF EXISTS t_alias_src;
DROP TABLE IF EXISTS t_not_mergetree;
DROP TABLE IF EXISTS t_source_access;
DROP USER IF EXISTS $user_name;
DROP USER IF EXISTS $user_show_tables;
DROP USER IF EXISTS $user_show_columns;
DROP USER IF EXISTS $user_alias_both;
"
