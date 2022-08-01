#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS wv;
DROP TABLE IF EXISTS dst;

CREATE TABLE dst(count UInt64, market Int64 default 0, w_end DateTime('US/Samoa')) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32, market Int64, timestamp DateTime('US/Samoa')) ENGINE=MergeTree ORDER BY tuple();

CREATE WINDOW VIEW wv TO dst WATERMARK=ASCENDING AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1, 1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));
INSERT INTO mt VALUES (1, 2, toDateTime('1990/01/01 12:00:01', 'US/Samoa'));
INSERT INTO mt VALUES (1, 3, toDateTime('1990/01/01 12:00:02', 'US/Samoa'));
INSERT INTO mt VALUES (1, 4, toDateTime('1990/01/01 12:00:05', 'US/Samoa'));
INSERT INTO mt VALUES (1, 5, toDateTime('1990/01/01 12:00:06', 'US/Samoa'));
INSERT INTO mt VALUES (1, 6, toDateTime('1990/01/01 12:00:10', 'US/Samoa'));
INSERT INTO mt VALUES (1, 7, toDateTime('1990/01/01 12:00:11', 'US/Samoa'));
INSERT INTO mt VALUES (1, 8, toDateTime('1990/01/01 12:00:30', 'US/Samoa'));
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT count, market, w_end FROM dst ORDER BY w_end, market;"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv;"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst;"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt;"
