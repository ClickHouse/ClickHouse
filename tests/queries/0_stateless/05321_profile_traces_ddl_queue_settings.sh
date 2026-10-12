#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hosts executing a queued DDL entry have no profile trace receiver, and an older host rejects
# `send_profile_traces` as `UNKNOWN_SETTING`. So the setting is removed from the carriers the statement
# itself applies (its trailing `SETTINGS` clause and the storage clause) and from the `AS SELECT` of
# `CREATE TABLE ... AS SELECT`, both for `ON CLUSTER` and for a `Replicated` database. A stored view
# definition keeps its own SQL settings.

DB="$CLICKHOUSE_DATABASE"
CLIENT="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none"

$CLIENT -m -q "
    CREATE TABLE $DB.ctas ON CLUSTER test_shard_localhost ENGINE = Memory SETTINGS send_profile_traces = 1
        AS SELECT 1 AS x SETTINGS send_profile_traces = 1;
    CREATE TABLE $DB.storage ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory
        SETTINGS min_rows_to_keep = 0, send_profile_traces = 1;
    ALTER TABLE $DB.storage ON CLUSTER test_shard_localhost ADD COLUMN y UInt8 SETTINGS send_profile_traces = 1;
    CREATE MATERIALIZED VIEW $DB.mv ON CLUSTER test_shard_localhost ENGINE = Memory
        AS SELECT x FROM $DB.storage SETTINGS send_profile_traces = 0;
    ALTER TABLE $DB.mv ON CLUSTER test_shard_localhost MODIFY QUERY SELECT x FROM $DB.storage WHERE x > 0 SETTINGS send_profile_traces = 1
        SETTINGS allow_experimental_alter_materialized_view_structure = 1, send_profile_traces = 1;
"

# Hide the generated UUIDs and the database name.
normalize()
{
    sed -E 's/ UUID \\?'"'"'[0-9a-f-]+\\?'"'"'//g' | sed "s/$DB/db/g"
}

echo 'ON CLUSTER'
$CLICKHOUSE_CLIENT -q "
    SELECT query FROM (SELECT DISTINCT entry, query FROM system.distributed_ddl_queue WHERE position(query, '$DB.') > 0)
    ORDER BY entry
    FORMAT TSVRaw
" | normalize

RDB="${DB}_replicated"
$CLIENT -m -q "
    CREATE DATABASE $RDB ENGINE = Replicated('/test/05321/$RDB', 's1', 'r1');
    CREATE TABLE $RDB.ctas ENGINE = Memory SETTINGS send_profile_traces = 1 AS SELECT 1 AS x SETTINGS send_profile_traces = 1;
    CREATE MATERIALIZED VIEW $RDB.mv ENGINE = Memory AS SELECT x FROM $RDB.ctas SETTINGS send_profile_traces = 0;
    ALTER TABLE $RDB.mv MODIFY QUERY SELECT x FROM $RDB.ctas WHERE x > 0 SETTINGS send_profile_traces = 1
        SETTINGS allow_experimental_alter_materialized_view_structure = 1, send_profile_traces = 1;
    SELECT * FROM $RDB.ctas;
"

echo 'Replicated'
$CLICKHOUSE_CLIENT -q "
    SELECT extract(value, 'query: ([^\n]*)') FROM system.zookeeper
    WHERE path = '/test/05321/$RDB/log' AND position(value, 'query: ') > 0 AND extract(value, 'query: ([^\n]*)') != ''
    ORDER BY name
    FORMAT TSVRaw
" | normalize

$CLICKHOUSE_CLIENT -q "DROP DATABASE $RDB SYNC"
