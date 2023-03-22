#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'test MergeTree with uuid'
${CLICKHOUSE_CLIENT} -q "drop table if exists 02681_undrop_uuid sync;"
uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")
${CLICKHOUSE_CLIENT} -q "create table 02681_undrop_uuid UUID '$uuid' (id Int32) Engine=MergeTree() order by id;"
${CLICKHOUSE_CLIENT} -q "insert into 02681_undrop_uuid values (1),(2),(3);"
${CLICKHOUSE_CLIENT} -q "drop table 02681_undrop_uuid settings database_atomic_wait_for_drop_and_detach_synchronously = 0;"
${CLICKHOUSE_CLIENT} -q "select table from system.marked_dropped_tables where table = '02681_undrop_uuid' limit 1;"
${CLICKHOUSE_CLIENT} -q "undrop table 02681_undrop_uuid UUID '$uuid' settings allow_experimental_undrop_table_query = 1;"
${CLICKHOUSE_CLIENT} -q "select * from 02681_undrop_uuid order by id;"
${CLICKHOUSE_CLIENT} -q "drop table 02681_undrop_uuid sync;"
