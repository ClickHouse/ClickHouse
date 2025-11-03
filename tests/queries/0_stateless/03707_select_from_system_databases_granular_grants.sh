#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

array=()
count=2000

for (( i=0; i<$count; i++ ));
do
    array+=("testgr_$(echo $RANDOM$RANDOM | tr '[0-9]' '[a-z]')")
done

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS u1"
$CLICKHOUSE_CLIENT -q "CREATE USER u1"

query_to_create_databases=

for name in "${array[@]}"
do
    query_to_create_databases+="${query_to_create_databases:+ PARALLEL WITH }"
    query_to_create_databases+="CREATE DATABASE $name ENGINE=Memory"
done

$CLICKHOUSE_CLIENT -q "$query_to_create_databases"

half_count=$((count / 2))

for ((i=0; i<$half_count; i++));
do
    name=${array[i]}
    query_to_grant_show_databases+="${query_to_grant_show_databases:+, }"
    query_to_grant_show_databases+="SHOW DATABASES ON $name.*"
done

query_to_grant_show_databases="GRANT $query_to_grant_show_databases TO u1"
$CLICKHOUSE_CLIENT -q "$query_to_grant_show_databases"

start_time=$(date +%s%3N)  # start time in milliseconds
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.databases WHERE name LIKE 'testgr%' ORDER BY name FORMAT NULL"
end_time=$(date +%s%3N)  # # end time in milliseconds
default_duration_ms=$((end_time - start_time))  # duration in milliseconds

start_time=$(date +%s%3N)  # start time in milliseconds
$CLICKHOUSE_CLIENT --user u1 -q "SELECT name FROM system.databases WHERE name LIKE 'testgr%' ORDER BY name FORMAT NULL"
end_time=$(date +%s%3N)  # # end time in milliseconds
u1_duration_ms=$((end_time - start_time))  # duration in milliseconds

query_to_drop_databases=

for name in "${array[@]}"
do
    query_to_drop_databases+="${query_to_drop_databases:+ PARALLEL WITH }"
    query_to_drop_databases+="DROP DATABASE $name"
done

$CLICKHOUSE_CLIENT -q "$query_to_drop_databases"

$CLICKHOUSE_CLIENT -q "DROP USER u1"

echo "SELECT FROM system.databases took $default_duration_ms milliseconds for user default"
echo "SELECT FROM system.databases took $u1_duration_ms milliseconds for user u1"
