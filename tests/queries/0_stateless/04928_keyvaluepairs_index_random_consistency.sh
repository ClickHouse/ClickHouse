#!/usr/bin/env bash
# Randomized consistency check for the keyValuePairs text index: using the index (granule pruning) must
# never change a query's result versus a full scan. Data is randomized per run (small key/value space so
# predicates actually match); the same set of literal-needle queries is run once with use_skip_indexes = 1
# and once with = 0, and the two result sets must be identical. optimize_functions_to_subcolumns is pinned
# so both sides lower m['key'] the same way (the comparison is index-on vs index-off, not accessor forms).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SEED=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now())")

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_kv_rand;
CREATE TABLE t_kv_rand (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
-- Each row gets 1..4 entries drawn from keys k0..k4 and values v0..v5 (duplicates possible).
INSERT INTO t_kv_rand
SELECT
    number,
    mapFromArrays(
        arrayMap(i -> concat('k', toString(cityHash64(number, i, ${SEED}) % 5)), range(1 + cityHash64(number, ${SEED}) % 4)),
        arrayMap(i -> concat('v', toString(cityHash64(number, i, 7, ${SEED}) % 6)), range(1 + cityHash64(number, ${SEED}) % 4)))
FROM numbers(400);
"

# Literal-needle queries so the index actually engages. Sum a hash of the matching ids per predicate.
QUERIES=""
for k in k0 k1 k2 k3 k4; do
    for v in v0 v1 v2 v3 v4 v5; do
        QUERIES+="SELECT 'eq ${k}=${v}', sum(cityHash64(id)), count() FROM t_kv_rand WHERE m['${k}'] = '${v}';"
    done
    QUERIES+="SELECT 'in ${k}', sum(cityHash64(id)), count() FROM t_kv_rand WHERE m['${k}'] IN ('v0', 'v2', 'v4');"
    QUERIES+="SELECT 'ck ${k}', sum(cityHash64(id)), count() FROM t_kv_rand WHERE mapContainsKey(m, '${k}');"
done
for v in v0 v1 v2 v3 v4 v5; do
    QUERIES+="SELECT 'cv ${v}', sum(cityHash64(id)), count() FROM t_kv_rand WHERE mapContainsValue(m, '${v}');"
done

ON=$($CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns 0 --use_skip_indexes 1 -mn -q "${QUERIES}")
OFF=$($CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns 0 --use_skip_indexes 0 -mn -q "${QUERIES}")

if [ "${ON}" = "${OFF}" ]; then
    echo "Consistent"
else
    echo "MISMATCH (seed=${SEED})"
    diff <(echo "${ON}") <(echo "${OFF}")
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_kv_rand"
