#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(time DateTime, colA String, colB String) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(colA String, colB String) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst AS SELECT tumbleStart(w_id) AS time, colA, colB FROM mt GROUP BY tumble(now(), INTERVAL '1' SECOND, 'US/Samoa') AS w_id, colA, colB;

INSERT INTO mt VALUES ('test1', 'test2');
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "1" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT colA, colB FROM dst"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst"
