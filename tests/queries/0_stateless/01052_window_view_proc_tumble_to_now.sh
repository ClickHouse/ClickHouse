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

CREATE TABLE ${db}.dst(count UInt64) Engine=MergeTree ORDER BY tuple();
CREATE TABLE ${db}.mt(a Int32) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW ${db}.wv TO ${db}.dst AS SELECT count(a) AS count FROM ${db}.mt GROUP BY tumble(now('US/Samoa'), INTERVAL '10' SECOND, 'US/Samoa') AS wid;
INSERT INTO ${db}.mt VALUES (1);

EOF

for _ in {1..100}; do
	$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count(*) FROM ${db}.dst" | grep -q "1" && echo 'OK' && break
	sleep .5
done

$CLICKHOUSE_CLIENT "${opts[@]}" --query="SELECT count FROM ${db}.dst"

$CLICKHOUSE_CLIENT --query="DROP DATABASE ${db}"
