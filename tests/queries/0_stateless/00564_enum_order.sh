#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP TABLE IF EXISTS enum";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "CREATE TABLE enum (x Enum8('a' = 1, 'bcdefghijklmno' = 0)) ENGINE = Memory";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "INSERT INTO enum VALUES ('a')";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT * FROM enum";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP TABLE enum";
