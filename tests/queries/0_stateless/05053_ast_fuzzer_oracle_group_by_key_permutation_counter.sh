#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
#
# Proves that `QueryOracleChecker::checkGroupByKeyPermutation` actually runs, via the
# dedicated `ASTFuzzerOracleGroupByKeyPermutationChecks` profile event. With
# `ast_fuzzer_runs = 1` a random mutation may drop a GROUP BY key (leaving < 2 keys) and
# silently skip the oracle, so we retry until the global counter increases.
# no-parallel: required because the proof event is server-global.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_gbperm_counter;
    CREATE TABLE oracle_gbperm_counter (a UInt8, b UInt8, c UInt8, v Int64) ENGINE = MergeTree ORDER BY (a, b);
    INSERT INTO oracle_gbperm_counter SELECT number % 5, number % 7, number % 3, number FROM numbers(300);
"

get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events WHERE event = 'ASTFuzzerOracleGroupByKeyPermutationChecks'"
}

before=$(get_counter)
after=$before

for _ in $(seq 1 100)
do
    $CLICKHOUSE_CLIENT --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        SELECT a, b, c, count() FROM oracle_gbperm_counter GROUP BY a, b, c ORDER BY a, b, c;
    " >/dev/null 2>/dev/null

    after=$(get_counter)
    if [[ "$after" -gt "$before" ]]
    then
        break
    fi
done

if [[ "$after" -gt "$before" ]]
then
    echo "GROUP BY key permutation oracle ran"
else
    echo "GROUP BY key permutation oracle never ran: counter stayed at $after"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_gbperm_counter"
