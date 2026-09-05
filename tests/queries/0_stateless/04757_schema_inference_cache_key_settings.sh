#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each carrier below is probed in BOTH orders, because an order-insensitive probe cannot
# detect a missing cache-key field: whichever query runs first decides the cached type.
# Every order gets its OWN file so the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: SchemaCache::tryGetImpl drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

for suffix in a b; do
    printf '1e5\n'  > "${T}_exp_${suffix}.tsv"
    printf '1e5\n'  > "${T}_exp_${suffix}.csv"
    printf 'x=1e5'  > "${T}_exp_${suffix}.form"
    printf '{"x":1e5}\n' > "${T}_exp_${suffix}.json"
done
touch -d "$AGE" "${T}"_*

# --- input_format_try_infer_exponent_floats -----------------------------------------------
# Each pair must report Float64 for the exp=1 query and String for the exp=0 query,
# whichever ran first.
for fmt_file in "TSV tsv" "CSV csv" "Form form"; do
    set -- $fmt_file
    echo "-- $1 exponent, exp=1 first"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_exp_a.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 1;
        DESC file('${T}_exp_a.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 0;" | awk '{print $2}'
    echo "-- $1 exponent, exp=0 first"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_exp_b.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 0;
        DESC file('${T}_exp_b.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 1;" | awk '{print $2}'
done

# The value channel, not just the type: a poisoned Float64 prints 100000 instead of 1e5.
echo "-- TSV exponent value, exp=1 first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT c1 FROM file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 1;
    SELECT c1 FROM file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 0;"

# Values uses the Quoted escaping rule. Only the permissive direction is observable: at
# exp=0 inference throws, and a throwing inference caches nothing.
echo "-- Values (Quoted) exponent, exp=1 first, second query must fail"
printf '(1e5)\n' > "${T}_exp_q.values"
touch -d "$AGE" "${T}_exp_q.values"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_q.values', 'Values') SETTINGS input_format_try_infer_exponent_floats = 1;
    DESC file('${T}_exp_q.values', 'Values') SETTINGS input_format_try_infer_exponent_floats = 0;" \
    2>&1 | grep -oE 'Nullable\(Float64\)|ONLY_NULLS_WHILE_READING_SCHEMA'

# JSON must NOT be keyed on this setting: tryReadFloat short-circuits it for JSON, so the
# verdict is identical at both values. This row keeps a later change from adding it there.
# The type alone cannot detect that: it is identical whether or not the field is in the key.
# Asserting that JSON produces ONE cache entry for both values is what pins the omission.
echo "-- JSON exponent control, type must be identical at both values"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_a.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 1;
    DESC file('${T}_exp_a.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 0;" | awk '{print $2}'
echo "-- JSON exponent control, both values must share one cache entry"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_b.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 1 FORMAT Null;
    DESC file('${T}_exp_b.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 0 FORMAT Null;
    SELECT count(), countDistinct(additional_format_info) FROM system.schema_inference_cache;"

rm -f "${T}"_*
