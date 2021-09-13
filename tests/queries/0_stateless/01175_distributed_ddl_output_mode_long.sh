#!/usr/bin/env bash
# Tags: long, distributed, no-msan, no-replicated-database
# Tag no-msan: issue 21600
# Tag no-replicated-database: ON CLUSTER is not allowed

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_OUT=$(mktemp "$CURDIR/01175_distributed_ddl_output_mode_long.XXXXXX")
trap 'rm -f ${TMP_OUT:?}' EXIT

# We execute a distributed DDL query with timeout 1 to check that one host is unavailable and will time out and other complete successfully.
# But sometimes one second is not enough even for healthy host to succeed. Repeat the test in this case.
function run_until_out_contains()
{
    PATTERN=$1
    shift

    for _ in {1..20}
    do
        "$@" > "$TMP_OUT" 2>&1
        if grep -q "$PATTERN" "$TMP_OUT"
        then
            cat "$TMP_OUT"
            break
        fi
    done
}


$CLICKHOUSE_CLIENT -q "drop table if exists none;"
$CLICKHOUSE_CLIENT -q "drop table if exists throw;"
$CLICKHOUSE_CLIENT -q "drop table if exists null_status;"
$CLICKHOUSE_CLIENT -q "drop table if exists never_throw;"

$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "select value from system.settings where name='distributed_ddl_output_mode';"
# Ok
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "create table none on cluster test_shard_localhost (n int) engine=Memory;"
# Table exists
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=none -q "create table none on cluster test_shard_localhost (n int) engine=Memory;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"
# Timeout

run_until_out_contains 'There are 1 unfinished hosts' $CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=none -q "drop table if exists none on cluster test_unavailable_shard;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/"


$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "create table throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=throw -q "create table throw on cluster test_shard_localhost (n int) engine=Memory format Null;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"

run_until_out_contains 'There are 1 unfinished hosts' $CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=throw -q "drop table if exists throw on cluster test_unavailable_shard;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//" | sed "s/Watching task .* is executing longer/Watching task <task> is executing longer/"


$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=null_status_on_timeout -q "create table null_status on cluster test_shard_localhost (n int) engine=Memory format Null;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"

run_until_out_contains '9000	0		' $CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=null_status_on_timeout -q "drop table if exists null_status on cluster test_unavailable_shard;"


$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "select value from system.settings where name='distributed_ddl_output_mode';"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;"
$CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=600 --distributed_ddl_output_mode=never_throw -q "create table never_throw on cluster test_shard_localhost (n int) engine=Memory;" 2>&1 | sed "s/DB::Exception/Error/g" | sed "s/ (version.*)//"

run_until_out_contains '9000	0		' $CLICKHOUSE_CLIENT --output_format_parallel_formatting=0 --distributed_ddl_task_timeout=1 --distributed_ddl_output_mode=never_throw -q "drop table if exists never_throw on cluster test_unavailable_shard;"


$CLICKHOUSE_CLIENT -q "drop table if exists none;"
$CLICKHOUSE_CLIENT -q "drop table if exists throw;"
$CLICKHOUSE_CLIENT -q "drop table if exists null_status;"
$CLICKHOUSE_CLIENT -q "drop table if exists never_throw;"
