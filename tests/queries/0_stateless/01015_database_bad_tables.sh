#!/usr/bin/env bash
# Tags: log-engine
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

db="db_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db;"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $db.\`таблица_со_странным_названием\` (a UInt64, b UInt64) ENGINE = Log;"
$CLICKHOUSE_CLIENT -q "INSERT INTO $db.\`таблица_со_странным_названием\` VALUES (1, 1);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $db;"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $db;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $db;"
