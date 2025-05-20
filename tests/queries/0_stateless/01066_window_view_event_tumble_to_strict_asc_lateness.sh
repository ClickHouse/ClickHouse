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
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS wv;

CREATE TABLE dst(count UInt64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst WATERMARK=STRICTLY_ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1, '1990/01/01 12:00:00');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:02');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:06');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:04');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:03');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:07');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:10');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:11');
EOF

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT * FROM dst ORDER BY w_end, count;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE wv;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE mt;"
$CLICKHOUSE_CLIENT "${opts[@]}" --query="DROP TABLE dst;"
