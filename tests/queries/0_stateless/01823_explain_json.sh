#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--enable_analyzer=1"
)
$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json = 1, description = 0 SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw"
echo "--------"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "explain json = 1, description = 0, header = 1 select 1, 2 + dummy FORMAT TSVRaw" 2> /dev/null | grep Header -m 1 -A 8

echo "--------"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json = 1, actions = 1, header = 1, description = 0
                       SELECT quantile(0.2)(number), sumIf(number, number > 0) from numbers(2) group by number, number + 1 FORMAT TSVRaw
                      " | grep Aggregating -A 36

echo "--------"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json = 1, actions = 1, description = 0
                       SELECT x, y from numbers(2) array join [number, 1] as x, [number + 1] as y  FORMAT TSVRaw
                      " | grep ArrayJoin -A 2

echo "--------"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json = 1, actions = 1, description = 0
                       SELECT distinct intDiv(number, 2), intDiv(number, 3) from numbers(10) FORMAT TSVRaw
                      " | grep Distinct -A 1

echo "--------"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json = 1, actions = 1, description = 0
                       SELECT number + 1 from numbers(10) order by number desc, number + 1 limit 3 FORMAT TSVRaw
                      " | grep "Sort Description" -A 12
