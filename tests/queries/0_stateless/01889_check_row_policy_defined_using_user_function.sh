#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop user if exists u_01889"
${CLICKHOUSE_CLIENT} -q "drop role if exists r_01889"
${CLICKHOUSE_CLIENT} -q "drop policy if exists t_01889_filter on t_01889"
${CLICKHOUSE_CLIENT} -q "create user u_01889 identified with plaintext_password by 'dfsdffdf5t123'"
${CLICKHOUSE_CLIENT} -q "revoke all on *.* from u_01889"
${CLICKHOUSE_CLIENT} -q "create role r_01889"
${CLICKHOUSE_CLIENT} -q "create table t_01889(a Int64, user_id String) Engine=MergeTree order by a"
${CLICKHOUSE_CLIENT} -q "insert into t_01889 select number, 'u_01889' from numbers(1000)"
${CLICKHOUSE_CLIENT} -q "insert into t_01889 select number, 'xxxxxxx' from numbers(1000)"
${CLICKHOUSE_CLIENT} -q "grant select on t_01889 to r_01889"
${CLICKHOUSE_CLIENT} -q "create row policy t_01889_filter ON t_01889 FOR SELECT USING user_id = user() TO r_01889"
${CLICKHOUSE_CLIENT} -q "grant r_01889 to u_01889"
${CLICKHOUSE_CLIENT} -q "alter user u_01889 default role r_01889 settings none"

${CLICKHOUSE_CLIENT_BINARY} --database=${CLICKHOUSE_DATABASE} --user=u_01889 --password=dfsdffdf5t123 --query="select count() from t_01889"

${CLICKHOUSE_CLIENT} -q "drop user u_01889"
${CLICKHOUSE_CLIENT} -q "drop policy t_01889_filter on t_01889"
${CLICKHOUSE_CLIENT} -q "drop role r_01889"
${CLICKHOUSE_CLIENT} -q "drop table t_01889"
