#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create tables
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    INSERT INTO mt SELECT number, today(), '' FROM numbers(1e6);
    INSERT INTO mt SELECT number, today()-60, '' FROM numbers(1e5);

    CREATE TABLE replacing ( A Int64, D Date, S String ) ENGINE ReplacingMergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;
    INSERT INTO replacing SELECT number, today(), '' FROM numbers(1e6);
    INSERT INTO replacing SELECT number, today()-60, '' FROM numbers(1e5);

    CREATE TABLE replacing_ver ( A Int64, D Date, S String ) ENGINE = ReplacingMergeTree(D) PARTITION BY toYYYYMM(D) ORDER BY A;
    CREATE TABLE collapsing_ver ( ID UInt64, Sign Int8, Version UInt8 ) ENGINE = VersionedCollapsingMergeTree(Sign, Version) ORDER BY ID;
"

# Convert tables
$CLICKHOUSE_CLIENT -n -q "
    ATTACH TABLE mt AS REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE replacing AS REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE replacing_ver AS REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE collapsing_ver AS REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }

    DETACH TABLE mt;
    DETACH TABLE replacing;
    DETACH TABLE replacing_ver;
    DETACH TABLE collapsing_ver;

    ATTACH TABLE mt AS REPLICATED;
    ATTACH TABLE replacing AS REPLICATED;
    ATTACH TABLE replacing_ver AS REPLICATED;
    ATTACH TABLE collapsing_ver AS REPLICATED;

    SYSTEM RESTORE REPLICA mt;
    SYSTEM RESTORE REPLICA replacing;
    SYSTEM RESTORE REPLICA replacing_ver;
    SYSTEM RESTORE REPLICA collapsing_ver;
"

# Check tables
# ARGS_i is used to suppress style check for not using {database} in ZK path
# ATTACH AS REPLICATED uses only default path, so there is nothing else we can do
ARGS_1="(\\\\'/clickhouse/tables/{uuid}/{shard}\\\\', \\\\'{replica}\\\\')"
${CLICKHOUSE_CLIENT} --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'mt'" \
| grep -c "ReplicatedMergeTree$ARGS_1 PARTITION BY toYYYYMM(D) ORDER BY A"
${CLICKHOUSE_CLIENT} --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'replacing'" \
| grep -c "ReplicatedReplacingMergeTree$ARGS_1 PARTITION BY toYYYYMM(D) ORDER BY A"
ARGS_2="(\\\\'/clickhouse/tables/{uuid}/{shard}\\\\', \\\\'{replica}\\\\', D)"
${CLICKHOUSE_CLIENT} --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'replacing_ver'" \
| grep -c "ReplicatedReplacingMergeTree$ARGS_2 PARTITION BY toYYYYMM(D) ORDER BY A"
ARGS_3="(\\\\'/clickhouse/tables/{uuid}/{shard}\\\\', \\\\'{replica}\\\\', Sign, Version)"
${CLICKHOUSE_CLIENT} --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'collapsing_ver'" \
| grep -c "ReplicatedVersionedCollapsingMergeTree$ARGS_3 ORDER BY ID"
echo

$CLICKHOUSE_CLIENT --echo --query="SELECT is_readonly FROM system.replicas WHERE database=currentDatabase() AND table='mt'" \
| grep -c "0"
$CLICKHOUSE_CLIENT --echo --query="SELECT is_readonly FROM system.replicas WHERE database=currentDatabase() AND table='replacing'" \
| grep -c "0"
$CLICKHOUSE_CLIENT --echo --query="SELECT is_readonly FROM system.replicas WHERE database=currentDatabase() AND table='replacing_ver'" \
| grep -c "0"
$CLICKHOUSE_CLIENT --echo --query="SELECT is_readonly FROM system.replicas WHERE database=currentDatabase() AND table='collapsing_ver'" \
| grep -c "0"
echo

# Convert tables back
# Get zk paths
MT_ZK_PATH=$($CLICKHOUSE_CLIENT --query="SELECT zookeeper_path FROM system.replicas WHERE database=currentDatabase() AND table='mt'")
REPLACING_ZK_PATH=$($CLICKHOUSE_CLIENT --query="SELECT zookeeper_path FROM system.replicas WHERE database=currentDatabase() AND table='replacing'")
REPLACING_VER_ZK_PATH=$($CLICKHOUSE_CLIENT --query="SELECT zookeeper_path FROM system.replicas WHERE database=currentDatabase() AND table='replacing_ver'")
COLLAPSING_VER_ZK_PATH=$($CLICKHOUSE_CLIENT --query="SELECT zookeeper_path FROM system.replicas WHERE database=currentDatabase() AND table='collapsing_ver'")

# Restored replica has no table_shared_id node in ZK
# DROP REPLICA will log warning while deleting this node
$CLICKHOUSE_CLIENT -n -q "
    ATTACH TABLE mt AS NOT REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE replacing AS NOT REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE replacing_ver AS NOT REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }
    ATTACH TABLE collapsing_ver AS NOT REPLICATED; -- { serverError TABLE_ALREADY_EXISTS }

    DETACH TABLE mt;
    DETACH TABLE replacing;
    DETACH TABLE replacing_ver;
    DETACH TABLE collapsing_ver;

    ATTACH TABLE mt AS NOT REPLICATED;
    ATTACH TABLE replacing AS NOT REPLICATED;
    ATTACH TABLE replacing_ver AS NOT REPLICATED;
    ATTACH TABLE collapsing_ver AS NOT REPLICATED;

    SET send_logs_level = 'error';
    SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$MT_ZK_PATH';
    SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$REPLACING_ZK_PATH';
    SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$REPLACING_VER_ZK_PATH';
    SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$COLLAPSING_VER_ZK_PATH';
    SET send_logs_level = 'trace';
"

# Check tables
${CLICKHOUSE_CLIENT} --echo --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'mt'" \
| grep -c "MergeTree PARTITION BY toYYYYMM(D) ORDER BY A"
${CLICKHOUSE_CLIENT} --echo --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'replacing'" \
| grep -c "ReplacingMergeTree PARTITION BY toYYYYMM(D) ORDER BY A"
${CLICKHOUSE_CLIENT} --echo --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'replacing_ver'" \
| grep -c "ReplacingMergeTree(D) PARTITION BY toYYYYMM(D) ORDER BY A"
${CLICKHOUSE_CLIENT} --echo --query="SELECT engine_full FROM system.tables WHERE database=currentDatabase() AND name = 'collapsing_ver'" \
| grep -c "VersionedCollapsingMergeTree(Sign, Version) ORDER BY ID"
