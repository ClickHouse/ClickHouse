#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json(id UInt64, data Object('JSON')) \
    ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0"

cat << EOF | $CLICKHOUSE_CLIENT -q  "INSERT INTO t_json FORMAT CSV"
1,{"k1":"aa","k2":{"k3":"bb","k4":"c"}}
2,{"k1":"ee","k5":"ff"}
EOF

cat << EOF | $CLICKHOUSE_CLIENT -q  "INSERT INTO t_json FORMAT CSV"
3,{"k5":"foo"}
EOF

$CLICKHOUSE_CLIENT -q "SELECT id, data.k1, data.k2.k3, data.k2.k4, data.k5 FROM t_json ORDER BY id"
$CLICKHOUSE_CLIENT -q "SELECT name, column, type \
    FROM system.parts_columns WHERE table = 't_json' AND database = '$CLICKHOUSE_DATABASE' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json(id UInt64, data Object('JSON')) \
    ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0"

cat << EOF | $CLICKHOUSE_CLIENT -q  "INSERT INTO t_json FORMAT CSV"
1,{"k1":[{"k2":"aaa","k3":[{"k4":"bbb"},{"k4":"ccc"}]},{"k2":"ddd","k3":[{"k4":"eee"},{"k4":"fff"}]}]}
EOF

$CLICKHOUSE_CLIENT -q "SELECT id, data.k1.k2, data.k1.k3.k4 FROM t_json ORDER BY id"

$CLICKHOUSE_CLIENT -q "SELECT name, column, type \
    FROM system.parts_columns WHERE table = 't_json' AND database = '$CLICKHOUSE_DATABASE' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json"
