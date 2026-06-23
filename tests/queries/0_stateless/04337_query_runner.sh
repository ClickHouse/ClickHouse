#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS user_04337;
CREATE TABLE query_runner_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE query_runner (query String, database String, settings Map(LowCardinality(String), String), delay_microseconds UInt64) ENGINE = QueryRunner SETTINGS mode = 'synchronous';

SELECT 'validation';
CREATE TABLE query_runner_bad (q String) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (database String) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query UInt64) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String, settings Map(String, UInt64)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String, delay_microseconds Int64) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String, database String ALIAS query) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner('arg'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS mode = 'sometimes'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS threads = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS max_queue_size = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'nonexistent_cluster_04337'; -- { serverError CLUSTER_DOESNT_EXIST }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS unknown_setting = 1; -- { serverError UNKNOWN_SETTING }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' DEFINER = default SQL SECURITY DEFINER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS shard_num = 2; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard_num = 5; -- { serverError BAD_ARGUMENTS }
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE query_runner_bad (query LowCardinality(String)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
SELECT * FROM query_runner; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE query_runner ADD COLUMN extra String; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE query_runner DROP COLUMN database; -- { serverError NOT_IMPLEMENTED }

SELECT 'dispatch, batch survives failing and resultful queries, database routing, settings, logging';
INSERT INTO query_runner (query) VALUES ('SELECT 1'), ('SELECT throwIf(1)'), ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)'), ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (2)');
INSERT INTO query_runner (query, database) SELECT 'INSERT INTO query_runner_target VALUES (3)', 'system';
INSERT INTO query_runner (query, database, delay_microseconds) SELECT 'INSERT INTO query_runner_target VALUES (3)', currentDatabase(), 1000;
INSERT INTO query_runner (query, settings) VALUES ('SELECT throwIf(1, ''04337_local_fail'') SETTINGS log_comment = ''04337_marker''', {'log_comment': '04337_marker'});
-- system-routed insert hits no such table there, so the count stays 3 (rows 1, 2, 3).
SELECT x FROM query_runner_target ORDER BY x;
SYSTEM FLUSH LOGS query_log;
SELECT 'logged internal failure', count() > 0, any(is_internal AND exception_code != 0) FROM system.query_log WHERE log_comment = '04337_marker' AND type = 'ExceptionWhileProcessing';

SELECT 'sql security';
TRUNCATE TABLE query_runner_target;
CREATE USER user_04337;
CREATE TABLE query_runner_definer (query String, database String, settings Map(String, String), delay_microseconds UInt64) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2, max_queue_size = 100 DEFINER = user_04337 SQL SECURITY DEFINER;
SHOW CREATE TABLE query_runner_definer;
DETACH TABLE query_runner_definer;
ATTACH TABLE query_runner_definer;
CREATE TABLE query_runner_invoker (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY INVOKER;
GRANT INSERT ON query_runner_invoker TO user_04337;
EOF

${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_definer (query) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)')"
${CLICKHOUSE_CLIENT} --user user_04337 -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_invoker VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)')"
${CLICKHOUSE_CLIENT} -q "SELECT 'no grants', count() FROM ${CLICKHOUSE_DATABASE}.query_runner_target"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.query_runner_target TO user_04337"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_definer (query) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)')"
${CLICKHOUSE_CLIENT} --user user_04337 -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_invoker VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (2)')"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'with grants', count() FROM query_runner_target;

SELECT 'sql security none';
CREATE TABLE query_runner_none (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY NONE;
INSERT INTO query_runner_none VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (3)');
SELECT 'none runs without grants', count() FROM query_runner_target;

SELECT 'no alter escape';
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO user_04337;
GRANT TABLE ENGINE ON QueryRunner TO user_04337;
ALTER TABLE query_runner_definer MODIFY SQL SECURITY INVOKER; -- { serverError NOT_IMPLEMENTED }
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_evil (query String) ENGINE = QueryRunner DEFINER = default SQL SECURITY DEFINER -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_evil (query String) ENGINE = QueryRunner SQL SECURITY NONE -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'identity';
CREATE TABLE query_runner_user_check (u String) ENGINE = MergeTree ORDER BY tuple();
GRANT INSERT ON query_runner_user_check TO user_04337;
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_invoker VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_user_check SELECT currentUser()')"
${CLICKHOUSE_CLIENT} -q "SELECT 'invoker currentUser', u FROM ${CLICKHOUSE_DATABASE}.query_runner_user_check"
${CLICKHOUSE_CLIENT} <<EOF
INSERT INTO query_runner_definer (query) VALUES ('SELECT ''qr_definer_logcheck''');
SYSTEM FLUSH LOGS query_log;
SELECT 'invoker log', user = 'user_04337', initial_user = 'user_04337', authenticated_user = 'user_04337' FROM system.query_log WHERE query LIKE '%query_runner_user_check SELECT currentUser()' AND is_internal AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'definer log', user = 'user_04337', initial_user = 'user_04337', authenticated_user = currentUser() FROM system.query_log WHERE query LIKE '%qr_definer_logcheck%' AND is_internal AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'roles: definer uses its default roles, invoker uses the inserter enabled roles';
CREATE ROLE role_04337_def, role_04337_inv;
CREATE TABLE role_target (x UInt64, who String) ENGINE = MergeTree ORDER BY x;
GRANT INSERT ON role_target TO role_04337_def, role_04337_inv;
CREATE USER user_04337_def DEFAULT ROLE role_04337_def;
GRANT role_04337_def TO user_04337_def;
CREATE USER user_04337_inv;
GRANT role_04337_inv TO user_04337_inv;
SET DEFAULT ROLE NONE TO user_04337_inv;
CREATE TABLE qr_role_def (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' DEFINER = user_04337_def SQL SECURITY DEFINER;
CREATE TABLE qr_role_inv (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY INVOKER;
GRANT INSERT ON qr_role_inv TO user_04337_inv;
INSERT INTO qr_role_def VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.role_target VALUES (1, ''def'')');
EOF
${CLICKHOUSE_CLIENT} --user user_04337_inv --multiquery -q "SET ROLE role_04337_inv; INSERT INTO ${CLICKHOUSE_DATABASE}.qr_role_inv VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.role_target VALUES (2, ''inv'')')"
${CLICKHOUSE_CLIENT} <<EOF
SELECT 'role rows', count() FROM role_target;
DROP TABLE qr_role_def, qr_role_inv, role_target;
DROP USER user_04337_def, user_04337_inv;
DROP ROLE role_04337_def, role_04337_inv;

SELECT 'cluster';
CREATE TABLE query_runner_cluster_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE query_runner_cluster (query String, database String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard_num = 1, mode = 'synchronous';
DETACH TABLE query_runner_cluster;
ATTACH TABLE query_runner_cluster;
INSERT INTO query_runner_cluster SELECT 'INSERT INTO query_runner_cluster_target VALUES (100)', currentDatabase();
SELECT 'cluster routes', x FROM query_runner_cluster_target;
SYSTEM FLUSH LOGS query_log;
SELECT 'cluster source-internal dest-initial', countIf(is_internal AND is_initial_query), countIf(NOT is_internal AND is_initial_query) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE 'INSERT INTO query_runner_cluster_target VALUES%';
SELECT 'cluster attribution', user = '', initial_user = '', authenticated_user = currentUser() FROM system.query_log WHERE is_internal AND is_initial_query AND type = 'QueryFinish' AND query LIKE 'INSERT INTO query_runner_cluster_target VALUES%' LIMIT 1;
INSERT INTO query_runner_cluster (query, database) SELECT 'SELECT * FROM nonexistent_04337_cluster_fail', currentDatabase();
SYSTEM FLUSH LOGS query_log;
SELECT 'cluster exception logged', count() > 0, any(is_internal AND is_initial_query AND exception_code != 0) FROM system.query_log WHERE query LIKE '%nonexistent_04337_cluster_fail%' AND type = 'ExceptionWhileProcessing';

SELECT 'cluster multi-db routing';
CREATE DATABASE db_04337_a;
CREATE DATABASE db_04337_b;
CREATE TABLE db_04337_a.t (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db_04337_b.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO query_runner_cluster (query, database) VALUES ('INSERT INTO t VALUES (1)', 'db_04337_a'), ('INSERT INTO t VALUES (2)', 'db_04337_b');
SELECT 'cluster multi-db', (SELECT count() FROM db_04337_a.t), (SELECT count() FROM db_04337_b.t), (SELECT x FROM db_04337_a.t), (SELECT x FROM db_04337_b.t);
DROP DATABASE db_04337_a;
DROP DATABASE db_04337_b;

SELECT 'asynchronous is the default mode';
TRUNCATE TABLE query_runner_target;
CREATE TABLE query_runner_async (query String) ENGINE = QueryRunner;
INSERT INTO query_runner_async VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)');
EOF
for _ in {1..300}; do
    res=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM query_runner_target")
    [ "$res" == "1" ] && break
    sleep 0.2
done
echo "async rows $res"

${CLICKHOUSE_CLIENT} -q "GRANT ALLOW SQL SECURITY NONE ON *.* TO user_04337"
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_user_none (query String) ENGINE = QueryRunner SQL SECURITY NONE" && echo "none created with grant"

${CLICKHOUSE_CLIENT} -q "SELECT 'cluster cancel on detach'"
CANCEL_MARKER="04337_cluster_cancel_${CLICKHOUSE_DATABASE}"
STUCK_QUERY="SELECT sleepEachRow(3) FROM numbers(100500) SETTINGS max_block_size = 1, log_comment = ''${CANCEL_MARKER}''"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_cancel (query String, database String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard_num = 1, mode = 'asynchronous'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_cancel SELECT '${STUCK_QUERY}', currentDatabase()"
started=0
for _ in {1..150}; do
    n=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE '%${CANCEL_MARKER}%' AND query NOT LIKE '%system.processes%'")
    [ "$n" -ge 1 ] && { started=1; break; }
    sleep 0.2
done
echo "cluster query started $started"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.query_runner_cancel"
echo "detached"
gone=0
for _ in {1..150}; do
    n=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE '%${CANCEL_MARKER}%' AND query NOT LIKE '%system.processes%'")
    [ "$n" -eq 0 ] && { gone=1; break; }
    sleep 0.2
done
echo "cluster query gone $gone"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "SELECT 'cluster query cancelled', count() = 1, any(type = 'ExceptionWhileProcessing'), any(exception_code = 735) FROM system.query_log WHERE NOT is_internal AND query LIKE 'SELECT sleepEachRow%${CANCEL_MARKER}%' AND type = 'ExceptionWhileProcessing'"
${CLICKHOUSE_CLIENT} -q "SELECT 'cluster cancel no source terminal', count() FROM system.query_log WHERE is_internal AND query LIKE 'SELECT sleepEachRow%${CANCEL_MARKER}%' AND type IN ('QueryFinish', 'ExceptionWhileProcessing')"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.query_runner_cancel"

${CLICKHOUSE_CLIENT} -q "SELECT 'delay wait wakeup on detach'"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_delay (query String, delay_microseconds UInt64) ENGINE = QueryRunner SETTINGS threads = 2, mode = 'asynchronous'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_delay VALUES ('SELECT 1', 100500000000000), ('SELECT 1', 100500000000000), ('SELECT 1', 100500000000000), ('SELECT 1', 100500000000000)"
sleep 3
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.query_runner_delay"
echo "delay workers unblocked"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.query_runner_delay"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'definer is protected from drop while in use, droppable after';
DROP USER user_04337; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE query_runner_definer;
DROP USER user_04337;

SELECT 'definer dependency follows a table rename';
CREATE USER user_04337_rename;
CREATE TABLE query_runner_rename (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' DEFINER = user_04337_rename SQL SECURITY DEFINER;
RENAME TABLE query_runner_rename TO query_runner_renamed;
DROP USER user_04337_rename; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE query_runner_renamed;
DROP USER user_04337_rename;

DROP TABLE query_runner_user_check, query_runner_user_none, query_runner_async, query_runner_cluster, query_runner_cluster_target, query_runner_none, query_runner_invoker, query_runner, query_runner_target;
EOF
