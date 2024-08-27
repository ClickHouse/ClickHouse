#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--enable_analyzer=0"
)

$CLICKHOUSE_CLIENT "${opts[@]}" <<EOF
SET allow_experimental_window_view = 1;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(count UInt64) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst AS SELECT count(a) AS count FROM mt GROUP BY tumble(now('US/Samoa'), INTERVAL '10' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1);
EOF

for _ in {1..100}; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM dst" | grep -q "1" && echo 'OK' && break
	sleep .5
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count FROM dst"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE wv"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE mt"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE dst"
