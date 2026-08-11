#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: vector_similarity needs usearch, absent in the ENABLE_LIBRARIES=0 fast build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_types;
    CREATE TABLE t_hypo_types (a UInt64, s String, v Array(Float32)) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_types SELECT number, toString(number), [1, 2, 3] FROM numbers(10);
"

echo "--- every allowed type is accepted ---"
while read -r type
do
    if $CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE $type GRANULARITY 1; DROP HYPOTHETICAL INDEX hi ON t_hypo_types;" > /dev/null 2>&1
    then
        echo "accepted: $type"
    else
        echo "REJECTED: $type"
    fi
done <<'EOF'
minmax
set(100)
bloom_filter(0.01)
ngrambf_v1(3, 256, 2, 0)
tokenbf_v1(256, 2, 0)
sparse_grams(3, 100, 512, 2, 0)
EOF

echo "--- types outside the allowlist are rejected at CREATE ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (v) TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;" 2>&1 | grep -m1 -oE 'NOT_IMPLEMENTED'
$CLICKHOUSE_CLIENT -q "SET allow_experimental_full_text_index = 1; CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE text(tokenizer = 'array') GRANULARITY 1;" 2>&1 | grep -m1 -oE 'NOT_IMPLEMENTED'

echo "--- the rejection names the supported types ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (v) TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;" 2>&1 \
    | grep -m1 -oE 'Supported types: [a-z_, ]+'

echo "--- validate still runs before the allowlist check ---"
# 'hypothesis' is registered but its validator rejects it outright, so that error must win
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (a > 0) TYPE hypothesis GRANULARITY 1;" 2>&1 | grep -m1 -oE 'ILLEGAL_INDEX'
# an unregistered type is rejected by the factory lookup, not by the allowlist
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_types (s) TYPE no_such_index GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY'

echo "--- a schema change that invalidates the index degrades to not_applicable ---"
# minmax rejects Dynamic columns, so after the ALTER the stored description no longer validates.
# The estimate must report that per candidate instead of building an index from it
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_minmax ON t_hypo_types (s) TYPE minmax GRANULARITY 1;
    ALTER TABLE t_hypo_types MODIFY COLUMN s Dynamic SETTINGS mutations_sync = 2;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_types WHERE s = 'x';
" 2>&1 | grep -m1 -oE '^  status: +not_applicable' | awk '{$1=$1; print}'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_types;"
