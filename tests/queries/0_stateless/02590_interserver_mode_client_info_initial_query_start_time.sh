#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: interserver mode requires SSL
#
# Test that checks that some of ClientInfo correctly passed in inter-server mode.
# NOTE: we need .sh test (.sql is not enough) because queries on remote nodes does not have current_database = currentDatabase()
#
# Check-style suppression: select * from system.query_log where current_database = currentDatabase();

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_query_id() { random_str 10; }

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists buf;
    drop table if exists dist;
    drop table if exists data;

    create table data (key Int) engine=Memory();
    create table dist as data engine=Distributed(test_cluster_interserver_secret, currentDatabase(), data, key);
    create table dist_dist as data engine=Distributed(test_cluster_interserver_secret, currentDatabase(), dist, key);
    system stop distributed sends dist;
"

echo "SELECT"
query_id="$(get_query_id)"
# initialize connection, but actually if there are other tables that uses this
# cluster then, it will be created long time ago, but this is OK for this
# test, since we care about the difference between NOW() and there should
# not be any significant difference.
$CLICKHOUSE_CLIENT --prefer_localhost_replica=0 --query_id "$query_id" -q "select * from dist"
$CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
    system flush logs;
    select count(), count(distinct initial_query_start_time_microseconds) from system.query_log where type = 'QueryFinish' and initial_query_id = {query_id:String};
"

sleep 1

query_id="$(get_query_id)"
# this query (and all subsequent) should reuse the previous connection (at least most of the time)
$CLICKHOUSE_CLIENT --prefer_localhost_replica=0 --query_id "$query_id" -q "select * from dist"

$CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
    system flush logs;
    select count(), count(distinct initial_query_start_time_microseconds) from system.query_log where type = 'QueryFinish' and initial_query_id = {query_id:String};
"

echo "INSERT"
query_id="$(get_query_id)"
$CLICKHOUSE_CLIENT --prefer_localhost_replica=0 --query_id "$query_id" -m -q "
    insert into dist_dist values (1),(2);
    select * from data;
"

sleep 1
$CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "system flush distributed dist_dist"
sleep 1
$CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "system flush distributed dist"

echo "CHECK"
$CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
    select * from data order by key;
    system flush logs;
    select count(), count(distinct initial_query_start_time_microseconds) from system.query_log where type = 'QueryFinish' and initial_query_id = {query_id:String};
"
