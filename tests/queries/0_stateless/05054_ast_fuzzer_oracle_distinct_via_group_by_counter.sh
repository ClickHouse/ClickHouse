#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
#
# Proves that `QueryOracleChecker::checkDistinctViaGroupBy` actually runs, via the dedicated
# `ASTFuzzerOracleDistinctViaGroupByChecks` profile event. With `ast_fuzzer_runs = 1` a random
# mutation may drop the DISTINCT or add grouping/aggregates and silently skip the oracle, so we
# retry until the global counter increases.
# no-parallel: required because the proof event is server-global.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_distinct_gb_counter;
    CREATE TABLE oracle_distinct_gb_counter (a UInt8, b UInt8, v Int64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO oracle_distinct_gb_counter SELECT number % 7, number % 13, number FROM numbers(300);
"

get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events WHERE event = 'ASTFuzzerOracleDistinctViaGroupByChecks'"
}

before=$(get_counter)
after=$before

for _ in $(seq 1 100)
do
    $CLICKHOUSE_CLIENT --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        SELECT DISTINCT a, b FROM oracle_distinct_gb_counter WHERE v > 10;
    " >/dev/null 2>/dev/null

    after=$(get_counter)
    if [[ "$after" -gt "$before" ]]
    then
        break
    fi
done

if [[ "$after" -gt "$before" ]]
then
    echo "DISTINCT via GROUP BY oracle ran"
else
    echo "DISTINCT via GROUP BY oracle never ran: counter stayed at $after"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_distinct_gb_counter"
