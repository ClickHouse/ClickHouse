#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--allow_experimental_analyzer=0"
)

db="$(random_str 10)"

$CLICKHOUSE_CLIENT "${opts[@]}" --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};

CREATE TABLE ${db}.dst(count UInt64, market Int32, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE ${db}.mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY timestamp;

INSERT INTO ${db}.mt VALUES (1, 1, '1990/01/01 12:00:00');
INSERT INTO ${db}.mt VALUES (1, 2, '1990/01/01 12:00:01');
INSERT INTO ${db}.mt VALUES (1, 3, '1990/01/01 12:00:02');
INSERT INTO ${db}.mt VALUES (1, 4, '1990/01/01 12:00:05');
INSERT INTO ${db}.mt VALUES (1, 5, '1990/01/01 12:00:06');
INSERT INTO ${db}.mt VALUES (1, 6, '1990/01/01 12:00:10');
INSERT INTO ${db}.mt VALUES (1, 7, '1990/01/01 12:00:11');
INSERT INTO ${db}.mt VALUES (1, 8, '1990/01/01 12:00:30');

CREATE WINDOW VIEW ${db}.wv TO ${db}.dst WATERMARK=ASCENDING POPULATE AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM ${db}.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market ;
EOF

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${db}.dst" | grep -q "7" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --multiquery <<EOF
SELECT * FROM ${db}.dst ORDER BY market, w_end;
INSERT INTO ${db}.mt VALUES (1, 8, '1990/01/01 12:00:35');
INSERT INTO ${db}.mt VALUES (1, 8, '1990/01/01 12:00:37');
INSERT INTO ${db}.mt VALUES (1, 8, '1990/01/01 12:00:42');
SELECT '------';
EOF

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${db}.dst" | grep -q "9" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT * FROM ${db}.dst ORDER BY market, w_end;"

$CLICKHOUSE_CLIENT --query="DROP DATABASE ${db}"
