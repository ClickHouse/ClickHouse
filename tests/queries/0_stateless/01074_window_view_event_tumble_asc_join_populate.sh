#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS info;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(count UInt64, sum UInt64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE info(key Int32, value Int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO info VALUES (1, 2);

INSERT INTO mt VALUES (1, '1990/01/01 12:00:00');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:01');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:02');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:06');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:10');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:11');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:30');

CREATE WINDOW VIEW wv TO dst WATERMARK=ASCENDING POPULATE AS SELECT count(a) AS count, sum(info.value) as sum, tumbleEnd(wid) AS w_end FROM mt JOIN info ON mt.a = info.key GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT * FROM dst ORDER BY w_end;"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt"
$CLICKHOUSE_CLIENT --query="DROP TABLE info"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst"
