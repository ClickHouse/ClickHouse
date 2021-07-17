#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "drop table if exists none;"
$CLICKHOUSE_CLIENT -q "drop table if exists throw;"
$CLICKHOUSE_CLIENT -q "drop table if exists null_status;"
$CLICKHOUSE_CLIENT -q "drop table if exists never_throw;"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "select value from system.settings where name='distributed_ddl_output_mode';"
# Ok
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "create table none on cluster test_shard_localhost (n int) engine=Memory;"
# Table exists
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "create table none on cluster test_shard_localhost (n int) engine=Memory;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
# Timeout
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=none -q "drop table none on cluster test_unavailable_shard;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "create table throw on cluster test_shard_localhost (n int) engine=Memory format Null;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=throw -q "drop table throw on cluster test_unavailable_shard;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory format Null;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=null_status_on_timeout -q "drop table null_status on cluster test_unavailable_shard;"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=never_throw -q "drop table never_throw on cluster test_unavailable_shard;"

$CLICKHOUSE_CLIENT -q "drop table if exists none;"
$CLICKHOUSE_CLIENT -q "drop table if exists throw;"
$CLICKHOUSE_CLIENT -q "drop table if exists null_status;"
$CLICKHOUSE_CLIENT -q "drop table if exists never_throw;"
