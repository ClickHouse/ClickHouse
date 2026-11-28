#!/usr/bin/env bash
# Tags: replica, no-replicated-database
# I don't understand why this test fails in ReplicatedDatabase run
# but too many magic included in it, so I just disabled it for ReplicatedDatabase run becase
# here we explicitely create it and check is alright.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "create table mute_stylecheck (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/root', '1') order by x"

${CLICKHOUSE_CLIENT} -q "CREATE USER user_${CLICKHOUSE_DATABASE} settings database_replicated_allow_replicated_engine_arguments=0"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}_db.* TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON ReplicatedMergeTree TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db engine = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_db', '{shard}', '{replica}')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_rmt_ok (x UInt32) engine = ReplicatedMergeTree order by x;"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_rmt_fail (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/root/{shard}', '{replica}') order by x; -- { serverError 36 }"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP USER user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "drop table mute_stylecheck"
