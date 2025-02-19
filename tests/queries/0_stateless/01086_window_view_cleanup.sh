#!/usr/bin/env bash

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--enable_analyzer=0"
)

DATABASE_ORDINARY="${CLICKHOUSE_DATABASE}_ordinary"

$CLICKHOUSE_CLIENT "${opts[@]}" --allow_deprecated_database_ordinary=1 --multiquery "
    SET allow_experimental_window_view = 1;
    SET window_view_clean_interval = 1;

    DROP DATABASE IF EXISTS ${DATABASE_ORDINARY};
    CREATE DATABASE ${DATABASE_ORDINARY} ENGINE=Ordinary;

    CREATE TABLE ${DATABASE_ORDINARY}.dst(count UInt64, market Int32, w_end DateTime) Engine=MergeTree ORDER BY tuple();
    CREATE TABLE ${DATABASE_ORDINARY}.mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
    CREATE WINDOW VIEW ${DATABASE_ORDINARY}.wv TO ${DATABASE_ORDINARY}.dst WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM ${DATABASE_ORDINARY}.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market;

    INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));
    INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 2, toDateTime('1990/01/01 12:00:01', 'US/Samoa'));
    INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 3, toDateTime('1990/01/01 12:00:02', 'US/Samoa'));
    INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 4, toDateTime('1990/01/01 12:00:05', 'US/Samoa'));
    INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 5, toDateTime('1990/01/01 12:00:06', 'US/Samoa'));
"

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${DATABASE_ORDINARY}.\`.inner.wv\`" | grep -q "5" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT sleep(2);"

$CLICKHOUSE_CLIENT "${opts[@]}" --query="INSERT INTO ${DATABASE_ORDINARY}.mt VALUES (1, 6, toDateTime('1990/01/01 12:00:11', 'US/Samoa'));"

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${DATABASE_ORDINARY}.\`.inner.wv\`" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT market, wid FROM ${DATABASE_ORDINARY}.\`.inner.wv\` ORDER BY market, \`windowID(timestamp, toIntervalSecond('5'), 'US/Samoa')\` as wid";
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE ${DATABASE_ORDINARY}.wv SYNC;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE ${DATABASE_ORDINARY}.mt SYNC;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE ${DATABASE_ORDINARY}.dst SYNC;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP DATABASE ${DATABASE_ORDINARY} SYNC;"
