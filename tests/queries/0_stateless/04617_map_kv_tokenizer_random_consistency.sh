#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage
# This is a heavy randomized consistency check: it generates and runs a large number of small queries
# via many clickhouse-client invocations. Under the slow instrumented builds — and especially the flaky
# check, which re-runs a new test many times — it exceeds the per-test timeout. The keyValuePairs index
# code paths it covers are also exercised under sanitizers by the deterministic tests (04614/04616/04618/04619),
# so this fuzz test runs only in the regular (fast) builds.
# Randomized consistency check for the keyValuePairs text index: the index must never change the
# result of a mapContains* predicate versus a brute-force scan. Each predicate is run in four variants
# — {index on, index off} x {optimize_functions_to_subcolumns on, off} — over both a default-serialized
# and a bucketed-serialized Map column, on deterministic-random data with duplicate keys (built at the
# array level), arbitrary bytes, 63/64-byte trailer-boundary key lengths, and empty keys/values. All
# four variants of a given predicate must return the same set of matching rows.
#
# The m['key'] accessor families (equals, LIKE, startsWith, endsWith) are also checked, but only in the
# two optimize_functions_to_subcolumns=0 variants. There the accessor lowers to arrayElement, which for
# a duplicate key returns the first occurrence in the row — exactly the occurrence the index pins
# (is_rest=0) — so index-on and index-off must agree. The optimize_functions_to_subcolumns=1 accessor
# path is intentionally NOT compared: for duplicate keys the Map subcolumn returns the last occurrence
# while arrayElement returns the first, so the two subcolumn settings disagree independently of the
# index (a general Map property, not an index bug). See the commented-out block below.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

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

for TBL in t_def t_buckets; do
  for VAR in "idx_sub1:${SIDX}, optimize_functions_to_subcolumns=1" \
             "idx_sub0:${SIDX}, optimize_functions_to_subcolumns=0" \
             "noidx_sub1:${SNO}, optimize_functions_to_subcolumns=1" \
             "noidx_sub0:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''ck'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsKey(m, unhex(''' || hex(n) || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(mapKeys(m)) n FROM ${TBL} ORDER BY cityHash64(n) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''cv'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsValue(m, unhex(''' || hex(n) || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(mapValues(m)) n FROM ${TBL} ORDER BY cityHash64(n) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''ckl'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsKeyLike(m, unhex(''' || hex('%'||substring(n,1,3)||'%') || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(mapKeys(m)) n FROM ${TBL} ORDER BY cityHash64(n) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''cvl'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsValueLike(m, unhex(''' || hex(substring(n,1,2)||'%') || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(mapValues(m)) n FROM ${TBL} ORDER BY cityHash64(n) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''ckv'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsKeyValue(m, unhex(''' || hex(p.1) || '''), unhex(''' || hex(p.2) || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''ckvl'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE mapContainsKeyValueLike(m, unhex(''' || hex('%'||substring(p.1,1,3)||'%') || '''), ''%'') SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
  done

  # m['key'] accessor families: compared only at optimize_functions_to_subcolumns=0 (arrayElement path),
  # where the accessor returns the first occurrence of a duplicate key — the occurrence the index pins.
  # The key and value needles are LITERALS (unhex): the accessor subcolumn rewrite runs before scalar
  # folding, so a non-literal key would not engage the index. All four accessor families engage the index
  # at sub=0 (verified via EXPLAIN actions=1 -> __text_index_).
  for VAR in "idx_sub0:${SIDX}, optimize_functions_to_subcolumns=0" \
             "noidx_sub0:${SNO}, optimize_functions_to_subcolumns=0"; do
    vn="${VAR%%:*}"; vs="${VAR#*:}"
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''eq'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE m[unhex(''' || hex(p.1) || ''')] = unhex(''' || hex(p.2) || ''') SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''el'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE m[unhex(''' || hex(p.1) || ''')] LIKE unhex(''' || hex(p.2||'%') || ''') SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''esw'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE startsWith(m[unhex(''' || hex(p.1) || ''')], unhex(''' || hex(substring(p.2,1,2)) || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
    $CLICKHOUSE_CLIENT -q "SELECT 'INSERT INTO res SELECT ''${TBL}'',''eew'',''${vn}'',cityHash64(id) FROM ${TBL} WHERE endsWith(m[unhex(''' || hex(p.1) || ''')], unhex(''' || hex(substring(p.2, greatest(length(p.2)-1,1))) || ''')) SETTINGS ${vs};' FROM (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m),mapValues(m))) p FROM ${TBL} ORDER BY cityHash64(p.1, p.2) LIMIT 25) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -mn
  done

  # UNSTABLE — deliberately not run. At optimize_functions_to_subcolumns=1 the accessor lowers to the Map
  # subcolumn, which for a duplicate key returns the LAST occurrence, while arrayElement (sub=0) and the
  # index return the FIRST. So idx_sub1 vs noidx_sub1 would agree with each other but disagree with the
  # sub=0 pair, and mixing them into the same q would report a spurious mismatch. This divergence is a
  # general Map subcolumn-vs-arrayElement property, independent of the keyValuePairs index.
  #   for VAR in "idx_sub1:..." "noidx_sub1:..."; do ... same eq/el/esw/eew generators ... done
done

RESULT=$($CLICKHOUSE_CLIENT -q "SELECT tbl, q, groupArray(vh) FROM (SELECT tbl,q,variant,sum(h) AS vh FROM res GROUP BY tbl,q,variant) GROUP BY tbl,q HAVING uniqExact(vh) > 1 ORDER BY tbl,q")
if [ -z "$RESULT" ]; then echo "Consistent"; else echo "MISMATCH (seed=$SEED):"; echo "$RESULT"; fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_def; DROP TABLE t_buckets; DROP TABLE res;"
