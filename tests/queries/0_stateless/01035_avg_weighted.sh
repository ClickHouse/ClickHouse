#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 5), (2, 4), (3, 3), (4, 2), (5, 1)]) AS t));"
${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]) AS t));"

types=("Int8" "Int16" "Int32" "Int64" "Int128" "Int256"
       "UInt8" "UInt16" "UInt32" "UInt64" "UInt128" "UInt256"
       "Float32" "Float64"
       "Decimal32" "Decimal64" "Decimal128" "Decimal256")

for left in "${types[@]}"
do
    for right in "${types[@]}"
    do
        ${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, w) FROM values('x ${left}, w ${right}', (4, 1), (1, 0), (10, 2))"
        ${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, w) FROM values('x ${left}, w ${right}', (8, 1), (122, 0))"
        ${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, w) FROM values('x ${left}, w ${right}', (0, 0), (1, 0))"
    done
done

echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="SELECT avgWeighted(['string'], toFloat64(0))" 2>&1)" \
  | grep -c 'Code: 43. DB::Exception: .* DB::Exception:.* Types .* of arguments are non-conforming as arguments for aggregate function avgWeighted'
