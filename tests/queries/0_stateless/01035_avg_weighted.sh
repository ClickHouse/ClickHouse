#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 5), (2, 4), (3, 3), (4, 2), (5, 1)]) AS t));"
${CLICKHOUSE_CLIENT} --query="SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]) AS t));"

echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="SELECT avgWeighted(toDecimal64(0, 0), toFloat64(0))" 2>&1)" \
  | grep -c 'Code: 43. DB::Exception: .* DB::Exception:.* Different types .* of arguments for aggregate function avgWeighted'
