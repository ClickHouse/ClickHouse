#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "drop table if exists throw;"
$CLICKHOUSE_CLIENT -q "drop table if exists null_status;"
$CLICKHOUSE_CLIENT -q "drop table if exists never_throw;"

CLICKHOUSE_CLIENT_OPT=$(echo ${CLICKHOUSE_CLIENT_OPT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=fatal/g')

CLIENT="$CLICKHOUSE_CLIENT_BINARY $CLICKHOUSE_CLIENT_OPT --distributed_ddl_task_timeout=8 --distributed_ddl_output_mode=none"
$CLIENT -q "select value from system.settings where name='distributed_ddl_output_mode';"
# Ok
$CLIENT -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;"
# Table exists
$CLIENT -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;" 2>&1| grep -Fv "@ 0x" | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/exists.. /exists/"
# Timeout
$CLIENT -q "drop table throw on cluster test_unavailable_shard;" 2>&1| grep -Fv "@ 0x" | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/" | sed "s/background. /background./"

CLIENT="$CLICKHOUSE_CLIENT_BINARY $CLICKHOUSE_CLIENT_OPT --distributed_ddl_task_timeout=8 --distributed_ddl_output_mode=throw"
$CLIENT -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLIENT -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLIENT -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;" 2>&1| grep -Fv "@ 0x" | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/exists.. /exists/"
$CLIENT -q "drop table throw on cluster test_unavailable_shard;" 2>&1| grep -Fv "@ 0x" | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/" | sed "s/background. /background./"

CLIENT="$CLICKHOUSE_CLIENT_BINARY $CLICKHOUSE_CLIENT_OPT --distributed_ddl_task_timeout=8 --distributed_ddl_output_mode=null_status_on_timeout"
$CLIENT -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLIENT -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory;"
$CLIENT -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory;" 2>&1| grep -Fv "@ 0x" | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/exists.. /exists/"
$CLIENT -q "drop table null_status on cluster test_unavailable_shard;"

CLIENT="$CLICKHOUSE_CLIENT_BINARY $CLICKHOUSE_CLIENT_OPT --distributed_ddl_task_timeout=8 --distributed_ddl_output_mode=never_throw"
$CLIENT -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLIENT -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLIENT -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;" 2>&1| sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
$CLIENT -q "drop table never_throw on cluster test_unavailable_shard;"
