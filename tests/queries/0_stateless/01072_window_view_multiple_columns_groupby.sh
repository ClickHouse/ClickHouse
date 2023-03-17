#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--allow_experimental_analyzer=0"
)

RAND_ID=$(shuf --input-range 10000000-99999999 --head-count=1)

$CLICKHOUSE_CLIENT "${opts[@]}" --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt_$RAND_ID;
DROP TABLE IF EXISTS dst_$RAND_ID;
DROP TABLE IF EXISTS wv_$RAND_ID;

CREATE TABLE dst_$RAND_ID(time DateTime, colA String, colB String) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt_$RAND_ID(colA String, colB String) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv_$RAND_ID TO dst_$RAND_ID AS SELECT tumbleStart(w_id) AS time, colA, colB FROM mt_$RAND_ID GROUP BY tumble(now(), INTERVAL '10' SECOND, 'US/Samoa') AS w_id, colA, colB;

INSERT INTO mt_$RAND_ID VALUES ('test1', 'test2');
EOF

for _ in {1..100}; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM dst_$RAND_ID" | grep -q "1" && echo 'OK' && break
	# sleep .5
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT colA, colB FROM dst_$RAND_ID"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE wv_$RAND_ID"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE mt_$RAND_ID"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE dst_$RAND_ID"
