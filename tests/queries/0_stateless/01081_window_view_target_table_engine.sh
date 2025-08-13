#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--enable_analyzer=0"
)

$CLICKHOUSE_CLIENT "${opts[@]}" <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS wv;

CREATE TABLE mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market;

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
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM wv" | grep -q "7" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT * FROM wv ORDER BY market, w_end;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE mt"
