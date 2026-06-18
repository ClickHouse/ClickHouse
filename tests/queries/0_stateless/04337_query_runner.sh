#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS query_runner_target;
DROP TABLE IF EXISTS query_runner;
DROP TABLE IF EXISTS query_runner_definer;
DROP TABLE IF EXISTS query_runner_invoker;
DROP TABLE IF EXISTS query_runner_none;
DROP TABLE IF EXISTS query_runner_cluster;
DROP TABLE IF EXISTS query_runner_cluster_target;
DROP TABLE IF EXISTS query_runner_async;
DROP TABLE IF EXISTS query_runner_user_none;
DROP USER IF EXISTS user_04337;
CREATE TABLE query_runner_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE query_runner (query String, database String, settings Map(LowCardinality(String), String), delay_microseconds UInt64) ENGINE = QueryRunner SETTINGS mode = 'synchronous';
EOF

${CLICKHOUSE_CLIENT} <<EOF
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
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' SQL SECURITY INVOKER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' SQL SECURITY DEFINER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' DEFINER = default SQL SECURITY DEFINER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS shard_num = 2; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard_num = 5; -- { serverError BAD_ARGUMENTS }
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE query_runner_bad (query String, delay_microseconds LowCardinality(UInt64)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE query_runner_bad (query LowCardinality(String)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
SELECT * FROM query_runner; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE query_runner ADD COLUMN extra String; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE query_runner DROP COLUMN database; -- { serverError NOT_IMPLEMENTED }
EOF

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'plain queries';
INSERT INTO query_runner (query) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)'), ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (2)');
SELECT x FROM query_runner_target ORDER BY x;

SELECT 'failing and resultful queries do not affect the batch';
INSERT INTO query_runner (query) VALUES ('SELECT 1'), ('SELECT throwIf(1)'), ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (3)');
SELECT count() FROM query_runner_target;

SELECT 'database column routes the query';
INSERT INTO query_runner (query, database) SELECT 'INSERT INTO query_runner_target VALUES (4)', 'system';
SELECT count() FROM query_runner_target;
INSERT INTO query_runner (query, database, delay_microseconds) SELECT 'INSERT INTO query_runner_target VALUES (4)', currentDatabase(), 1000;
SELECT count() FROM query_runner_target;

SELECT 'per-query settings and query_log';
INSERT INTO query_runner (query, settings) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (5)', {'log_comment': '04337_query_runner_marker'});
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, any(is_internal) FROM system.query_log WHERE log_comment = '04337_query_runner_marker' AND type = 'QueryFinish';

SELECT 'local query exception is logged';
INSERT INTO query_runner (query) VALUES ('SELECT throwIf(1, ''04337_local_fail'')');
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, any(is_internal AND exception_code != 0) FROM system.query_log WHERE query LIKE '%04337_local_fail%' AND type = 'ExceptionWhileProcessing';
EOF

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'sql security';
TRUNCATE TABLE query_runner_target;
CREATE USER user_04337;
CREATE TABLE query_runner_definer (query String, database String, settings Map(String, String), delay_microseconds UInt64) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2, max_queue_size = 100 DEFINER = user_04337 SQL SECURITY DEFINER;
SHOW CREATE TABLE query_runner_definer;
DETACH TABLE query_runner_definer;
ATTACH TABLE query_runner_definer;
CREATE TABLE query_runner_invoker (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY INVOKER;
GRANT INSERT ON query_runner_invoker TO user_04337;

SELECT 'definer and invoker without grants on the target';
INSERT INTO query_runner_definer (query) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)');
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_invoker VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)')"
${CLICKHOUSE_CLIENT} <<EOF
SELECT count() FROM query_runner_target;

SELECT 'definer and invoker with grants on the target';
GRANT INSERT ON query_runner_target TO user_04337;
INSERT INTO query_runner_definer (query) VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (1)');
SELECT count() FROM query_runner_target;
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_invoker VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (2)')"
${CLICKHOUSE_CLIENT} <<EOF
SELECT count() FROM query_runner_target;

SELECT 'sql security none';
CREATE TABLE query_runner_none (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY NONE;
INSERT INTO query_runner_none VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.query_runner_target VALUES (3)');
SELECT count() FROM query_runner_target;

SELECT 'definer and none require grants, no alter escape';
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO user_04337;
GRANT TABLE ENGINE ON QueryRunner TO user_04337;
ALTER TABLE query_runner_definer MODIFY SQL SECURITY INVOKER; -- { serverError NOT_IMPLEMENTED }
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_evil (query String) ENGINE = QueryRunner DEFINER = default SQL SECURITY DEFINER -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_evil (query String) ENGINE = QueryRunner SQL SECURITY NONE -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'cluster';
CREATE TABLE query_runner_cluster_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE query_runner_cluster (query String, database String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard_num = 1, mode = 'synchronous';
DETACH TABLE query_runner_cluster;
ATTACH TABLE query_runner_cluster;
INSERT INTO query_runner_cluster SELECT 'INSERT INTO query_runner_cluster_target VALUES (100)', currentDatabase();
SELECT x FROM query_runner_cluster_target;

SELECT 'source side internal, destination side initial';
SYSTEM FLUSH LOGS query_log;
SELECT countIf(is_internal AND is_initial_query), countIf(NOT is_internal AND is_initial_query) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE 'INSERT INTO query_runner_cluster_target VALUES%';

SELECT 'cluster query exception is received and logged';
INSERT INTO query_runner_cluster (query, database) SELECT 'SELECT * FROM nonexistent_04337_cluster_fail', currentDatabase();
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, any(is_internal AND is_initial_query AND exception_code != 0) FROM system.query_log WHERE query LIKE '%nonexistent_04337_cluster_fail%' AND type = 'ExceptionWhileProcessing';
EOF

${CLICKHOUSE_CLIENT} <<EOF
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
echo "$res"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'sql security none allowed with grant';
GRANT ALLOW SQL SECURITY NONE ON *.* TO user_04337;
EOF
${CLICKHOUSE_CLIENT} --user user_04337 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.query_runner_user_none (query String) ENGINE = QueryRunner SQL SECURITY NONE" && echo "created"

${CLICKHOUSE_CLIENT} <<EOF
SELECT 'definer is protected from drop while in use';
DROP USER user_04337; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE query_runner_definer;
DROP USER user_04337;
SELECT 'user dropped after definer table removed';

DROP TABLE query_runner_user_none;
DROP TABLE query_runner_async;
DROP TABLE query_runner_cluster;
DROP TABLE query_runner_cluster_target;
DROP TABLE query_runner_none;
DROP TABLE query_runner_invoker;
DROP TABLE query_runner;
DROP TABLE query_runner_target;
EOF
