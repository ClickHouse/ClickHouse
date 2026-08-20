#!/usr/bin/env bash
# Tags: no-asan
# no-asan: the many small index-on/index-off query pairs are slow enough under ASan+UBSan to exceed the
# per-test runtime limit; the consistency logic is architecture-independent, so covering it on other builds
# is sufficient.
# Randomized COMPOUND-predicate consistency check for the keyValuePairs text index. 04928 covers single
# predicates; this covers the RPN combination logic - AND / OR / NOT chains over the in-scope atoms
# (m['key'] = value and m['key'] IN (...)). Using the index (granule pruning + RPN combine) must never
# change a query's result versus a full scan, so the same compound queries are run once with
# use_skip_indexes = 1 and once with = 0, and the two result sets must be identical.
#
# Scope note: only the exact atoms this branch accelerates appear here. mapContainsKey/Value(Like),
# the m['key'] LIKE / startsWith forms and direct-read are deliberately absent - they are not accelerated
# yet, so mixing them in would test scan-vs-scan (vacuous) rather than the index path.
#
# optimize_functions_to_subcolumns is pinned to 0 so both sides lower m['key'] the same way (arrayElement,
# first-occurrence - the occurrence the index pins); the comparison is index-on vs index-off, not accessor
# forms. Data is randomized per run over a small key/value space so compound predicates actually match.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SEED=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now())")

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_kv_cmp;
CREATE TABLE t_kv_cmp (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
-- Each row gets 1..4 entries drawn from keys k0..k4 and values v0..v5 (duplicates possible).
INSERT INTO t_kv_cmp
SELECT
    number,
    mapFromArrays(
        arrayMap(i -> concat('k', toString(cityHash64(number, i, ${SEED}) % 5)), range(1 + cityHash64(number, ${SEED}) % 4)),
        arrayMap(i -> concat('v', toString(cityHash64(number, i, 7, ${SEED}) % 6)), range(1 + cityHash64(number, ${SEED}) % 4)))
FROM numbers(400);
"

# Compound queries over three rotated (key, value) needles per base (ka/va, kb/vb, kc/vc), so AND / OR / NOT
# chains cross-mix keys. Each family sums a hash of the matching ids so a differing row set changes the sum.
# The grid is kept small (each cell emits 9 statements run twice, index on and off) so the test stays well
# under the runtime limit under sanitizers; the rotations still span every key k0..k4 and values v0..v4,
# and the per-run randomized data keeps the matched row sets varying across runs.
QUERIES=""
for i in 0 1 2; do
    for j in 0 1 2; do
        ka="k$i";               va="v$j"
        kb="k$(((i + 1) % 5))";  vb="v$(((j + 1) % 6))"
        kc="k$(((i + 2) % 5))";  vc="v$(((j + 2) % 6))"
        tag="$i$j"
        eqa="m['$ka'] = '$va'"; eqb="m['$kb'] = '$vb'"; eqc="m['$kc'] = '$vc'"
        ina="m['$ka'] IN ('$va', '$vb')"
        QUERIES+="SELECT 'and $tag',    sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $eqa AND $eqb;"
        QUERIES+="SELECT 'or2 $tag',    sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $eqa OR $eqb;"
        QUERIES+="SELECT 'or3 $tag',    sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $eqa OR $eqb OR $eqc;"
        QUERIES+="SELECT 'not1 $tag',   sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $eqa AND NOT $eqb;"
        QUERIES+="SELECT 'not2 $tag',   sum(cityHash64(id)), count() FROM t_kv_cmp WHERE NOT $eqa OR $eqb;"
        QUERIES+="SELECT 'mix $tag',    sum(cityHash64(id)), count() FROM t_kv_cmp WHERE ($eqa OR $eqb) AND $eqc;"
        QUERIES+="SELECT 'in_and $tag', sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $ina AND $eqc;"
        QUERIES+="SELECT 'in_or $tag',  sum(cityHash64(id)), count() FROM t_kv_cmp WHERE $ina OR $eqc;"
        # OR-of-equals on one key must equal its IN(...) form; both engage the index. Fold the equivalence
        # into the on/off framework: the boolean is 1 iff the two forms agree, so a broken OR-vs-IN rewrite
        # would make the index-on side 0 while the scan side stays 1.
        QUERIES+="SELECT 'eqset $tag',
            (SELECT (sum(cityHash64(id)), count()) FROM t_kv_cmp WHERE m['$ka'] = '$va' OR m['$ka'] = '$vb') =
            (SELECT (sum(cityHash64(id)), count()) FROM t_kv_cmp WHERE m['$ka'] IN ('$va', '$vb'));"
    done
done

ON=$($CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns 0 --use_skip_indexes 1 -mn -q "${QUERIES}")
OFF=$($CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns 0 --use_skip_indexes 0 -mn -q "${QUERIES}")

if [ "${ON}" = "${OFF}" ]; then
    echo "Consistent"
else
    echo "MISMATCH (seed=${SEED})"
    diff <(echo "${ON}") <(echo "${OFF}")
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_kv_cmp"
