#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS wv;

CREATE TABLE mt(a Int32, market Int32, timestamp DateTime('US/Samoa')) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(mt.a) AS count, source.market, tumbleEnd(wid) AS w_end FROM mt AS source GROUP BY tumble(mt.timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market;

INSERT INTO mt VALUES (1, 1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));
INSERT INTO mt VALUES (1, 2, toDateTime('1990/01/01 12:00:01', 'US/Samoa'));
INSERT INTO mt VALUES (1, 3, toDateTime('1990/01/01 12:00:02', 'US/Samoa'));
INSERT INTO mt VALUES (1, 4, toDateTime('1990/01/01 12:00:05', 'US/Samoa'));
INSERT INTO mt VALUES (1, 5, toDateTime('1990/01/01 12:00:06', 'US/Samoa'));
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM wv" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --multiquery <<EOF
SELECT count, market, w_end FROM wv ORDER BY market, w_end;

DROP TABLE mt NO DELAY;
DETACH TABLE wv;
ATTACH TABLE wv;

INSERT INTO wv VALUES (1, 6, toDateTime('1990/01/01 12:00:10', 'US/Samoa'));
INSERT INTO wv VALUES (1, 7, toDateTime('1990/01/01 12:00:11', 'US/Samoa'));
INSERT INTO wv VALUES (1, 8, toDateTime('1990/01/01 12:00:30', 'US/Samoa'));
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM wv" | grep -q "5" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT count, market, w_end FROM wv ORDER BY market, w_end;"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv"
