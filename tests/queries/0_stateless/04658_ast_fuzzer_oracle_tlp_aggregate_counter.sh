#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
#
# Proves that `QueryOracleChecker::checkTLPAggregate` actually runs, via the
# dedicated `ASTFuzzerOracleTLPAggregateChecks` profile event. The companion
# smoke test (04256_04250) only asserts that oracle-enabled queries succeed —
# with `ast_fuzzer_runs = 1` a random mutation may destroy the aggregate shape
# (drop the WHERE, the aggregate, or the GROUP BY), silently skipping the
# aggregate-oracle path while the test still passes. Here we retry until the
# global counter increases, so the test fails when the path can never fire.
# no-parallel: required because the proof event is server-global.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_tlp_agg_counter;
    CREATE TABLE oracle_tlp_agg_counter (g UInt8, v Int64) ENGINE = MergeTree ORDER BY g;
    INSERT INTO oracle_tlp_agg_counter SELECT number % 5, number FROM numbers(200);
"

get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events WHERE event = 'ASTFuzzerOracleTLPAggregateChecks'"
}

before=$(get_counter)
after=$before

# The fuzzer's oracle mode preserves the topmost WHERE / aggregate shape, but a
# single mutation can still occasionally break oracle eligibility, so retry.
# One eligible pass increments the (monotonic, server-global) counter.
for _ in $(seq 1 100)
do
    # `send_logs_level = 'fatal'` suppresses expected error-level log lines from
    # random mutations that produce valid-but-nonsense queries (see 04256_04250).
    # No `FORMAT Null` here: the oracle skips queries carrying an explicit
    # FORMAT clause, so discard the output via redirection instead.
    $CLICKHOUSE_CLIENT --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        SELECT g, count(), min(v), max(v) FROM oracle_tlp_agg_counter WHERE v > 50 GROUP BY g ORDER BY g;
    " >/dev/null 2>/dev/null

    after=$(get_counter)
    if [[ "$after" -gt "$before" ]]
    then
        break
    fi
done

if [[ "$after" -gt "$before" ]]
then
    echo "TLP Aggregate oracle ran"
else
    echo "TLP Aggregate oracle never ran: counter stayed at $after"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_tlp_agg_counter"
