#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists src;
create table src (a Int32) engine = MergeTree() order by tuple();

drop table if exists dst;
create table dst (a Int32) engine = MergeTree() order by tuple();

drop table if exists mv;
create materialized view mv to dst (a Int32) as select * from src;
"

${CLICKHOUSE_CLIENT} -m --query "
drop table src;
backup database ${CLICKHOUSE_DATABASE} on cluster test_shard_localhost to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
drop table mv;
set allow_deprecated_database_ordinary=1;
restore table ${CLICKHOUSE_DATABASE}.mv on cluster test_shard_localhost from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists src;
create table src (a Int32) engine = MergeTree() order by tuple();

drop table if exists dst;
create table dst (a Int32) engine = MergeTree() order by tuple();

drop table if exists mv;
create materialized view mv to dst (a Int32) as select * from src;
"

${CLICKHOUSE_CLIENT} -m --query "
drop table src;
drop table dst;
backup database ${CLICKHOUSE_DATABASE} on cluster test_shard_localhost to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}2');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
drop table mv;
set allow_deprecated_database_ordinary=1;
restore table ${CLICKHOUSE_DATABASE}.mv on cluster test_shard_localhost from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}2');
" | grep -o "RESTORED"
