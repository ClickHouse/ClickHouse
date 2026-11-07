#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE DATABASE rdb1_$CLICKHOUSE_DATABASE ON CLUSTER test_shard_localhost ENGINE=Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')";
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE DATABASE rdb2_$CLICKHOUSE_DATABASE ON CLUSTER test_shard_localhost ENGINE=Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')";
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "RENAME DATABASE rdb2_$CLICKHOUSE_DATABASE to rdb3_$CLICKHOUSE_DATABASE ON CLUSTER test_shard_localhost"

$CLICKHOUSE_CLIENT -q "
SELECT
    db_name,
    t1.uuid = t2.uuid
FROM
(
    WITH '/clickhouse/databases/' AS prefix
    SELECT
        toUUID(substr(path, length(prefix) + 1)) AS uuid,
        value AS db_name
    FROM system.zookeeper
    WHERE (path IN (
        SELECT concat(path, name)
        FROM system.zookeeper
        WHERE path = prefix
    )) AND (name = 'first_replica_database_name')
) AS t1
INNER JOIN system.databases AS t2 USING (uuid)
WHERE db_name like '%$CLICKHOUSE_DATABASE%'
ORDER BY db_name
"

$CLICKHOUSE_CLIENT -q "DROP DATABASE rdb1_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "DROP DATABASE rdb3_$CLICKHOUSE_DATABASE"
