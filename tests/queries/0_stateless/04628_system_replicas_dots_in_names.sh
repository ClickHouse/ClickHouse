#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree, no-replicated-database: tables without UUIDs exist only in an Ordinary database.

# The first Ordinary database on the server makes it warn that the engine is deprecated.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tables in an Ordinary database have no UUID, so they are identified by their names.
# `db`.`t.u` and `db.t`.`u` must not be treated as the same table when reading system.replicas.

db_one="${CLICKHOUSE_DATABASE}_x"
db_two="${CLICKHOUSE_DATABASE}_x.t"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 -q "
    DROP DATABASE IF EXISTS \`$db_one\`;
    DROP DATABASE IF EXISTS \`$db_two\`;
    CREATE DATABASE \`$db_one\` ENGINE = Ordinary;
    CREATE DATABASE \`$db_two\` ENGINE = Ordinary;
    CREATE TABLE \`$db_one\`.\`t.u\` (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/one', 'r1') ORDER BY k;
    CREATE TABLE \`$db_two\`.\`u\` (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/two', 'r1') ORDER BY k;
"

$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(zookeeper_path, '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/', '')
    FROM system.replicas
    WHERE database IN ('$db_one', '$db_two')
    ORDER BY database
"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE \`$db_one\`;
    DROP DATABASE \`$db_two\`;
"

