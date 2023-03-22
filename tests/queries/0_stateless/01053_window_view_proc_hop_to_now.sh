#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(count UInt64) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst AS SELECT count(a) AS count FROM mt GROUP BY hop(now('US/Samoa'), INTERVAL '5' SECOND, INTERVAL '5' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1);
EOF

for _ in {1..100}; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "1" && echo 'OK' && break
	sleep .5
done

$CLICKHOUSE_CLIENT --query="SELECT count FROM dst"
$CLICKHOUSE_CLIENT --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst"
