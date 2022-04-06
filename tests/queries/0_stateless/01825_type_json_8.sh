#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_8"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_8 (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_8 FORMAT JSONAsObject"
{
    "k1": [
        [{"k2": 1, "k3": 2}, {"k2": 3, "k3": 4}],
        [{"k2": 5, "k3": 6}]
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT data, toTypeName(data) FROM t_json_8"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_json_8"

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_8 FORMAT JSONAsObject"
{
    "k1": [
        {"k2": [1, 3, 4, 5], "k3": [6, 7]},
        {"k2": [8], "k3": [9, 10, 11]}
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT data, toTypeName(data) FROM t_json_8"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_json_8"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_8"
