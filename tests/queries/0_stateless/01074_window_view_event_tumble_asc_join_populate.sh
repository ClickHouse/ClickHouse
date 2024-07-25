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

CREATE TABLE ${db}.dst(count UInt64, sum UInt64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE ${db}.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE ${db}.info(key Int32, value Int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO ${db}.info VALUES (1, 2);

INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:00');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:01');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:02');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:06');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:10');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:11');
INSERT INTO ${db}.mt VALUES (1, '1990/01/01 12:00:30');

CREATE WINDOW VIEW ${db}.wv TO ${db}.dst WATERMARK=ASCENDING POPULATE AS SELECT count(a) AS count, sum(${db}.info.value) as sum, tumbleEnd(wid) AS w_end FROM ${db}.mt JOIN ${db}.info ON ${db}.mt.a = ${db}.info.key GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;
EOF

while true; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${db}.dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT * FROM ${db}.dst ORDER BY w_end;"

$CLICKHOUSE_CLIENT --query="DROP DATABASE ${db}"
