#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: vector_similarity needs usearch, absent in the ENABLE_LIBRARIES=0 fast build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    SET enable_json_type = 1;
    DROP TABLE IF EXISTS t_hypo_types;
    CREATE TABLE t_hypo_types (a UInt64, s String, v Array(Float32), j JSON(key String)) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_types SELECT number, concat('value_', toString(number % 100)) AS s, [1, 2, 3], toJSONString(map('key', s)) FROM numbers(10000);
"

# Each allowed type must reach the estimator and be modelled empirically, not just pass CREATE
echo "--- every allowed type is estimated by EXPLAIN WHATIF ---"
while read -r type
do
    echo "$type: $($CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE $type GRANULARITY 1;
        EXPLAIN WHATIF SELECT count() FROM t_hypo_types WHERE s = 'value_42';
    " 2>&1 | grep -E '^  (status|source):' | awk '{print $2}' | tr '\n' ' ' | sed 's/ $//')"
done <<'EOF'
minmax
set(100)
bloom_filter(0.01)
ngrambf_v1(3, 256, 2, 0)
tokenbf_v1(256, 2, 0)
sparse_grams(3, 100, 512, 2, 0)
EOF

echo "jsonbf_v1: $($CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (j) TYPE jsonbf_v1 GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_types WHERE j.key = 'value_42';
" 2>&1 | grep -E '^  (status|source):' | awk '{print $2}' | tr '\n' ' ' | sed 's/ $//')"

echo "--- types outside the allowlist are rejected, and the error names what is supported ---"
# the client echoes the server message twice, so take the first match of each pattern
error=$($CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (v) TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;" 2>&1)
echo "$error" | grep -oE 'Supported types: [a-z0-9_, ]+' | head -1
echo "$error" | grep -oE 'NOT_IMPLEMENTED' | head -1
$CLICKHOUSE_CLIENT -q "SET allow_experimental_full_text_index = 1; CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE text(tokenizer = 'array') GRANULARITY 1;" 2>&1 | grep -oE 'NOT_IMPLEMENTED' | head -1

echo "--- validate still runs before the allowlist check ---"
# 'hypothesis' is registered but its validator rejects it outright, so that error must win
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (a > 0) TYPE hypothesis GRANULARITY 1;" 2>&1 | grep -oE 'ILLEGAL_INDEX' | head -1
# an unregistered type is rejected by the factory lookup, not by the allowlist
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE no_such_index GRANULARITY 1;" 2>&1 | grep -oE 'INCORRECT_QUERY' | head -1

echo "--- arguments the creator rejects fail at CREATE, not later ---"
# sparse_grams tokenizer bounds are enforced by the creator; validate only checks arity
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE sparse_grams(2, 100, 512, 2, 0) GRANULARITY 1;" 2>&1 | grep -oE 'BAD_ARGUMENTS' | head -1

echo "--- a schema change that invalidates the index degrades to not_applicable ---"
# minmax rejects Dynamic columns, so after the ALTER the stored description no longer validates
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_minmax ON t_hypo_types (s) TYPE minmax GRANULARITY 1;
    ALTER TABLE t_hypo_types MODIFY COLUMN s Dynamic SETTINGS mutations_sync = 2;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_types WHERE s = 'x';
" 2>&1 | grep -m1 -oE '^  status: +not_applicable' | awk '{$1=$1; print}'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_types;"
