#!/usr/bin/env bash
# Tags: no-old-analyzer

# Verifies `log_query_plans_probability`: a query that is not sampled still writes its
# `system.query_log` rows, but with an empty `query_plan` column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Runs 200 queries tagged by `log_comment`, with the given sampling probability.
function run_queries()
{
    local probability=$1
    local tag=$2
    for _ in {1..200}
    do
        echo "SELECT sum(number) FROM numbers(10) FORMAT Null;"
    done | $CLICKHOUSE_CLIENT --log_queries=1 --log_queries_probability=1 --log_query_plans=1 \
        --log_query_plans_probability="$probability" --log_comment="$tag"
}

run_queries 0 05316_never
run_queries 1 05316_always
run_queries 0.5 05316_half

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Every query writes its row. For the half-sampled queries, 200 draws at 0.5 fall outside of
# (50, 150) with a probability below 1e-12.
$CLICKHOUSE_CLIENT -q "
    SELECT
        log_comment,
        count(),
        multiIf(
            log_comment = '05316_half', toString(countIf(toJSONString(query_plan) != '{}') BETWEEN 51 AND 149),
            toString(countIf(toJSONString(query_plan) != '{}')))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05316_never', '05316_always', '05316_half')
    GROUP BY log_comment
    ORDER BY log_comment"
