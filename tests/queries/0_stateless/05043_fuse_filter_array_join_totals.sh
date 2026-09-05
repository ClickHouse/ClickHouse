#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The totals stream must not be filtered by the fused element predicate: a standalone FilterStep is a
# pass-through on totals, so fusing must keep the totals row identical to the unfused plan. Filter on the
# second element so the difference is visible (the totals row keeps the first, unfiltered element).
Q="SELECT a, s FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS) ARRAY JOIN [10, 20] AS a WHERE a = 20 ORDER BY a, s FORMAT TabSeparated"

ON=$($CLICKHOUSE_CLIENT --enable_analyzer=1 --query_plan_fuse_filter_into_array_join=1 -q "$Q")
OFF=$($CLICKHOUSE_CLIENT --enable_analyzer=1 --query_plan_fuse_filter_into_array_join=0 -q "$Q")

if [ "$ON" == "$OFF" ]; then
    echo "OK"
else
    echo "MISMATCH"
    diff <(echo "$OFF") <(echo "$ON")
fi
