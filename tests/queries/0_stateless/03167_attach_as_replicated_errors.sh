#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORDINARY_DB="ordinary_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;

    CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE mt;
    CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/rmt/s1', 'r1') PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE rmt;
    CREATE TABLE log ( A Int64, D Date, S String ) ENGINE Log;
    DETACH TABLE log;
    CREATE TABLE $ORDINARY_DB.mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    DETACH TABLE $ORDINARY_DB.mt;
"

echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE mt AS NOT REPLICATED" 2>&1)" \
  | grep -c 'Can not attach table as not replicated, table is already not replicated'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE rmt AS REPLICATED" 2>&1)" \
  | grep -c 'Can not attach table as replicated, table is already replicated'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE log AS REPLICATED" 2>&1)" \
  | grep -c 'Table engine conversion is supported only for MergeTree family engines'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE mt AS REPLICATED ON CLUSTER test_shard_localhost" 2>&1)" \
  | grep -c 'ATTACH AS \[NOT\] REPLICATED is not supported for ON CLUSTER queries'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE mt AS REPLICATED ( A Int64, D Date, S String ) ENGINE MergeTree ORDER BY A" 2>&1)" \
  | grep -c 'Attaching table as \[not\] replicated is supported only for short attach queries'
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE $ORDINARY_DB.mt AS REPLICATED" 2>&1)" \
  | grep -c 'Table engine conversion to replicated is supported only for Atomic databases'


${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    ATTACH TABLE $ORDINARY_DB.mt;
    DROP TABLE $ORDINARY_DB.mt;
    DROP DATABASE $ORDINARY_DB;
"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -q "CREATE TABLE already_exists_1 (id UInt32) ENGINE=MergeTree() ORDER BY id;"
UUID=$($CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 --query="SELECT uuid FROM system.tables WHERE database=currentDatabase() AND table='already_exists_1';")
ARGS_1="('/clickhouse/tables/$UUID/s1', 'r1')" # Suppress style check for zk path
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -q "CREATE TABLE already_exists_2 (id UInt32) ENGINE=ReplicatedMergeTree$ARGS_1 ORDER BY id;"
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -q "DETACH TABLE already_exists_1;"
echo "$(${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --server_logs_file=/dev/null --query="ATTACH TABLE already_exists_1 AS REPLICATED" 2>&1)" \
  | grep -c 'There already is an active replica with this replica path'
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -q "ATTACH TABLE already_exists_1 AS NOT REPLICATED;"
