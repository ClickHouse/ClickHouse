#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
#
# Approximate / order-dependent aggregates legitimately give a different answer
# when computed through partitioned `State`/`Merge` than in one pass, so the
# oracle must skip them. `topK` was skipped, but the equivalent `approx_top_k`
# family and aliases of skipped aggregates (`approx_top_count`, `array_agg`,
# `medianTDigest`, `any_value`, ...) were not and produced false
# `AST_FUZZER_ORACLE_MISMATCH` reports. Only such a mismatch is a failure here;
# other errors from random mutations are expected and ignored.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_approx_agg;
    CREATE TABLE oracle_approx_agg (g UInt8, v Int64) ENGINE = MergeTree ORDER BY g;
    INSERT INTO oracle_approx_agg SELECT number % 5, number FROM numbers(200);
"

for agg in "approx_top_k(v)" "approx_top_count(v)" "approx_top_sum(v, v)" "APPROX_TOP_K(v)" \
           "array_agg(v)" "medianTDigest(v)" "any_value(v)" "lttb(3)(v, v)"
do
    for _ in $(seq 1 10)
    do
        $CLICKHOUSE_CLIENT --query "
            SET send_logs_level = 'fatal';
            SET ast_fuzzer_runs = 1;
            SET ast_fuzzer_oracle = 1;
            SELECT g, count(), $agg FROM oracle_approx_agg WHERE v > 50 GROUP BY g ORDER BY g;
        " 2>&1 >/dev/null | grep -F "oracle mismatch" | sed "s/^/$agg: /"
    done
done

echo "OK"

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_approx_agg"
