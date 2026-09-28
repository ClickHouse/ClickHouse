#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `Remote` database over a named collection reads its remote database name from the collection on every start.
# After the collection is altered to an empty database name, the next start must still load the database: it lists
# no tables, a database chained to it can be created, and the process shuts down cleanly.

WORKING_FOLDER="${CLICKHOUSE_TMP}/05292_remote_database_engine_empty_database_name_replay"
rm -rf "${WORKING_FOLDER}"

${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}" -q "
    CREATE NAMED COLLECTION nc_05292 AS host = '127.0.0.1', database = 'default';
    CREATE DATABASE proxy ENGINE = Remote(nc_05292);
    ALTER NAMED COLLECTION nc_05292 SET database = ''"

${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}" -q "
    SELECT name, engine FROM system.databases WHERE name = 'proxy';
    SHOW TABLES FROM proxy;
    CREATE DATABASE chained ENGINE = Remote('127.0.0.1', 'proxy');
    SHOW TABLES FROM chained;
    SELECT count() FROM system.tables WHERE database IN ('proxy', 'chained')"
echo "exit code: $?"

rm -rf "${WORKING_FOLDER}"
