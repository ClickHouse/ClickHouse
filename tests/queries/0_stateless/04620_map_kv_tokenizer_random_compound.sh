#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage
# Heavy randomized consistency check (many small queries via many clickhouse-client invocations,
# ~40s in a regular build, dominated by process startup); it far exceeds the per-test timeout under the
# slow instrumented builds and the flaky check (which re-runs a new test many times). The keyValuePairs
# index code paths are covered under sanitizers by the deterministic tests (04614/04616/04618/04619),
# so this fuzz test runs only in the regular builds.
# Randomized consistency check for the keyValuePairs text index under COMPOUND predicates: AND / OR / NOT
# chains that cross-mix predicate families (mapContainsKey, mapContainsValue, mapContainsKeyValue, their
# LIKE forms and the m['key'] accessor) over two or three different (key, value) needles per query. The
# index must never change the result versus a brute-force scan, so the RPN combination logic
# (FUNCTION_AND / FUNCTION_OR / FUNCTION_NOT and the set / HAS_ANY_ELEMENTS path) is what is exercised
# here, on top of the single-predicate coverage in 04617.
#
# Each compound family is checked in four variants — {index on, index off} x {optimize_functions_to_subcolumns
# on, off} — over a default-serialized and a bucketed-serialized Map column. All variants of a family must
# return the same set of matching rows.
#
# The m['key'] accessor families are compared only at optimize_functions_to_subcolumns=0 (arrayElement,
# first-occurrence — the occurrence the index pins), same as 04617. The `eqset` family additionally
# compares the OR-of-equals chain against its IN(...) form: the two are semantically equal and both
# engage the index (each is an OR of exact m['key'] = vi lookups), so the OR-vs-IN rewrite must not
# change the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SEED=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now())")
# Character bytes are ASCII (1..127), not 0..255, to avoid https://github.com/ClickHouse/ClickHouse/issues/111232:
# vectorized position/LIKE misses a needle byte >= 0x80 over a multi-row String column, so the LIKE-family
# index-vs-scan comparisons here would spuriously diverge (the index answers byte-exactly, the scan does not).
# Revert `1 + ...%127` back to `...%256` once that is fixed. High bytes and NUL for the exact-match path are
# covered by the deterministic tests 04615 and 04619.
GENDATA="SELECT number, arrayZip(all_keys, arrayMap(j -> if(cityHash64(number,j,17,${SEED})%6=0,'', arrayStringConcat(arrayMap(i -> char(1 + cityHash64(number,j,i,3,${SEED})%127), range(1+(cityHash64(number,j,11,${SEED})%20))))), range(length(all_keys))))::Map(String,String) FROM (SELECT number, arrayConcat(base_keys, arraySlice(base_keys,1,1+(cityHash64(number,5,${SEED})%length(base_keys)))) AS all_keys FROM (SELECT number, arrayMap(k -> if(cityHash64(number,k,13,${SEED})%6=0,'', arrayStringConcat(arrayMap(i -> char(1 + cityHash64(number,k,i,${SEED})%127), range([1,50,63,64,65,127,200][1+(cityHash64(number,k,7,${SEED})%7)])))), range(1+(cityHash64(number,${SEED})%4))) AS base_keys FROM numbers(200)))"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_def; DROP TABLE IF EXISTS t_buckets; DROP TABLE IF EXISTS res;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_def (id UInt64, m Map(String,String), INDEX idx m TYPE text(tokenizer='keyValuePairs') GRANULARITY 1) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0, index_granularity=8; INSERT INTO t_def $GENDATA;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_buckets (id UInt64, m Map(String,String), INDEX idx m TYPE text(tokenizer='keyValuePairs') GRANULARITY 1) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0, index_granularity=8, map_serialization_version='with_buckets', max_buckets_in_map=4, map_buckets_strategy='constant'; INSERT INTO t_buckets $GENDATA;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE res (tbl String, q String, variant String, h UInt64) ENGINE=Memory;"

# use_query_condition_cache=0: the cache is an orthogonal accelerator that must not change results, and
# pinning it keeps this cross-variant consistency check deterministic (it is compared index-on vs -off).
SIDX="use_skip_indexes=1, query_plan_direct_read_from_text_index=1, use_text_index_like_evaluation_by_dictionary_scan=1, text_index_like_min_pattern_length=1, use_query_condition_cache=0"
SNO="use_skip_indexes=0, query_plan_direct_read_from_text_index=0, use_query_condition_cache=0"

# Atom fragments — each echoes a SQL string-concat expression that builds one predicate over pair $1 (a|b|c).
ck()  { printf "%s" "'mapContainsKey(m, unhex(''' || hex(${1}.1) || '''))'"; }
cv()  { printf "%s" "'mapContainsValue(m, unhex(''' || hex(${1}.2) || '''))'"; }
ckv() { printf "%s" "'mapContainsKeyValue(m, unhex(''' || hex(${1}.1) || '''), unhex(''' || hex(${1}.2) || '''))'"; }
ckl() { printf "%s" "'mapContainsKeyLike(m, unhex(''' || hex(concat('%', substring(${1}.1,1,3), '%')) || '''))'"; }
cvl() { printf "%s" "'mapContainsValueLike(m, unhex(''' || hex(concat(substring(${1}.2,1,2), '%')) || '''))'"; }
eq()  { printf "%s" "'m[unhex(''' || hex(${1}.1) || ''')] = unhex(''' || hex(${1}.2) || ''')'"; }
esw() { printf "%s" "'startsWith(m[unhex(''' || hex(${1}.1) || ''')], unhex(''' || hex(substring(${1}.2,1,2)) || '''))'"; }

# emit <family> <where-expr-sql>  — generates one INSERT per source row (25 needle triples) and runs them.
emit() {
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''${1}'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE ' || (${2}) || ' SETTINGS ${vs};' FROM ${SRC} FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
}

for TBL in t_def t_buckets; do
  # Three distinct (key, value) pairs per source row: a, and its two rotations b, c (cross-mixing).
  SRC="(WITH (SELECT groupArray(p) FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25)) AS arr SELECT arr[number+1] AS a, arr[((number+1)%length(arr))+1] AS b, arr[((number+2)%length(arr))+1] AS c FROM numbers(length(arr)))"

  # Group A: mapContains* only (order-independent) — compared across all four variants.
  for VAR in "idx_sub1:${SIDX}, optimize_functions_to_subcolumns=1" \
             "idx_sub0:${SIDX}, optimize_functions_to_subcolumns=0" \
             "noidx_sub1:${SNO}, optimize_functions_to_subcolumns=1" \
             "noidx_sub0:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    emit "and_ka"   "$(ck a) || ' AND ' || $(cv a)"
    emit "and_kb"   "$(ck a) || ' AND ' || $(cv b)"
    emit "or2"      "$(ck a) || ' OR ' || $(cv b)"
    emit "or3"      "$(ck a) || ' OR ' || $(cv b) || ' OR ' || $(ckv c)"
    emit "mix_oa"   "'(' || $(ck a) || ' OR ' || $(cv b) || ') AND ' || $(ckv a)"
    emit "mix_ao"   "$(ck a) || ' AND (' || $(cv b) || ' OR ' || $(ckv a) || ')'"
    emit "ckv_or3"  "$(ckv a) || ' OR ' || $(ckv b) || ' OR ' || $(ckv c)"
    emit "not_pair" "$(ck a) || ' AND NOT ' || $(ckv a)"
    emit "not_or"   "'NOT ' || $(ck a) || ' OR ' || $(cv b) || ' OR ' || $(ckv c)"
    emit "like_mix" "$(ckl a) || ' OR ' || $(cvl b) || ' OR ' || $(ckv c)"
  done

  # Group B: accessor-involving — compared only at optimize_functions_to_subcolumns=0 (see header).
  for VAR in "idx_sub0:${SIDX}, optimize_functions_to_subcolumns=0" \
             "noidx_sub0:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    emit "eq_and_k"   "$(eq a) || ' AND ' || $(ck b)"
    emit "esw_or_v"   "$(esw a) || ' OR ' || $(cv b)"
    # m['key'] IN (...) mixed into AND / OR chains with other families.
    emit "in_and_k"   "'m[unhex(''' || hex(a.1) || ''')] IN (unhex(''' || hex(a.2) || '''), unhex(''' || hex(b.2) || '''))' || ' AND ' || $(ck b)"
    emit "in_or_ckv"  "'m[unhex(''' || hex(a.1) || ''')] IN (unhex(''' || hex(a.2) || '''), unhex(''' || hex(b.2) || '''))' || ' OR ' || $(ckv c)"
  done

  # eqset: OR-of-equals chain vs its IN(...) form, both index on and off (four variants, all at sub0).
  # Same rows either way; both engage the index (an OR of exact m['key'] = vi lookups), so this checks
  # the OR-vs-IN rewrite does not change the answer.
  for VAR in "or_idx:${SIDX}, optimize_functions_to_subcolumns=0" \
             "or_noidx:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    emit "eqset" "$(eq a) || ' OR ' || '(' || 'm[unhex(''' || hex(a.1) || ''')] = unhex(''' || hex(b.2) || ''')' || ' OR ' || 'm[unhex(''' || hex(a.1) || ''')] = unhex(''' || hex(c.2) || ''')' || ')'"
  done
  for VAR in "in_idx:${SIDX}, optimize_functions_to_subcolumns=0" \
             "in_noidx:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    emit "eqset" "'m[unhex(''' || hex(a.1) || ''')] IN (unhex(''' || hex(a.2) || '''), unhex(''' || hex(b.2) || '''), unhex(''' || hex(c.2) || '''))'"
  done
done

RESULT=$($CLICKHOUSE_CLIENT -q "SELECT tbl, q, groupArray(vh) FROM (SELECT tbl,q,variant,sum(h) AS vh FROM res GROUP BY tbl,q,variant) GROUP BY tbl,q HAVING uniqExact(vh) > 1 ORDER BY tbl,q")
if [ -z "$RESULT" ]; then echo "Consistent"; else echo "MISMATCH (seed=$SEED):"; echo "$RESULT"; fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_def; DROP TABLE t_buckets; DROP TABLE res;"
