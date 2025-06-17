#!/usr/bin/env bash
# Tags: no-parallel, long
# Tag no-parallel: since someone may create table in system database

# Server may ignore some exceptions, but it still print exceptions to logs and (at least in CI) sends Error and Warning log messages to client
# making test fail because of non-empty stderr. Ignore such log messages.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Suppress style check: database=$CLICKHOUSE_DATABASE
$CLICKHOUSE_CLIENT -q "
    SELECT database || '.' || name FROM system.tables
    WHERE
        database in ('system', 'information_schema', 'INFORMATION_SCHEMA')
        AND name NOT IN ('zookeeper', 'models', 'coverage_log')
        AND name NOT LIKE '%\\_sender' AND name NOT LIKE '%\\_watcher'
" | xargs -P6 -i $CLICKHOUSE_CLIENT --allow_introspection_functions=1 --format=Null "SELECT * FROM {} LIMIT 10e3"
