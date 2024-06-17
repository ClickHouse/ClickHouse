#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


ORDINARY_DB="ordinary_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;

    CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE mt;
    CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/$ORDINARY_DB/rmt/s1', 'r1') PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE rmt;
    CREATE TABLE log ( A Int64, D Date, S String ) ENGINE Log;
    DETACH TABLE log;
    CREATE TABLE $ORDINARY_DB.mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE $ORDINARY_DB.mt;
"

echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE mt AS NOT REPLICATED" 2>&1)" \
  | grep -c 'Table is already not replicated'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE rmt AS REPLICATED" 2>&1)" \
  | grep -c 'Table is already replicated'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE log AS REPLICATED" 2>&1)" \
  | grep -c 'Table engine conversion is supported only for MergeTree family engines'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE mt AS REPLICATED ON CLUSTER test_shard_localhost" 2>&1)" \
  | grep -c 'ATTACH AS \[NOT\] REPLICATED is not supported for ON CLUSTER queries'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE $ORDINARY_DB.mt AS REPLICATED" 2>&1)" \
  | grep -c 'Table engine conversion to replicated is supported only for Atomic databases'


${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    ATTACH TABLE $ORDINARY_DB.mt;
    DROP TABLE $ORDINARY_DB.mt;
    DROP DATABASE $ORDINARY_DB;
"
