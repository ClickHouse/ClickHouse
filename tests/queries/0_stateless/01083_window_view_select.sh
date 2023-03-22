#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(count UInt64, market Int32, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market;

INSERT INTO mt VALUES (1, 1, '1990/01/01 12:00:00');
INSERT INTO mt VALUES (1, 2, '1990/01/01 12:00:01');
INSERT INTO mt VALUES (1, 3, '1990/01/01 12:00:02');
INSERT INTO mt VALUES (1, 4, '1990/01/01 12:00:05');
INSERT INTO mt VALUES (1, 5, '1990/01/01 12:00:06');
INSERT INTO mt VALUES (1, 6, '1990/01/01 12:00:10');
INSERT INTO mt VALUES (1, 7, '1990/01/01 12:00:11');
INSERT INTO mt VALUES (1, 8, '1990/01/01 12:00:30');
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM wv" | grep -q "7" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT * FROM wv ORDER BY market, w_end;"
$CLICKHOUSE_CLIENT --query="SELECT market FROM wv ORDER BY market, w_end;"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst"
