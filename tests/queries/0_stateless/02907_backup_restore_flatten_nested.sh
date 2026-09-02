#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists test;
set flatten_nested = 0;
create table test (test Array(Tuple(foo String, bar Float64))) ENGINE = MergeTree() ORDER BY tuple();
backup table ${CLICKHOUSE_DATABASE}.test on cluster test_shard_localhost to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "show create table test"

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists test2;
set flatten_nested = 0;
create table test2 (test Nested(foo String, bar Float64)) ENGINE = MergeTree() ORDER BY tuple();
backup table ${CLICKHOUSE_DATABASE}.test2 on cluster test_shard_localhost to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}2');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "show create table test2"

${CLICKHOUSE_CLIENT} -m --query "
drop table test sync;
set flatten_nested = 1;
restore table ${CLICKHOUSE_DATABASE}.test on cluster test_shard_localhost from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} --query "show create table test"

${CLICKHOUSE_CLIENT} -m --query "
drop table test2 sync;
set flatten_nested = 1;
restore table ${CLICKHOUSE_DATABASE}.test2 on cluster test_shard_localhost from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}2');
" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} --query "show create table test2"
