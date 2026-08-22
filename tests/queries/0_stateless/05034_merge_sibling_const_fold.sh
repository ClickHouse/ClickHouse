#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# merge()'s REGEXP argument only accepts a string literal, so the database name has to be
# interpolated by the shell rather than computed with currentDatabase(). Anchoring both regexps
# keeps a concurrent copy of this test, which creates the same table name in its own database,
# out of the selection.
DB_RE="^(system|${CLICKHOUSE_DATABASE})\$"
TABLE_RE='^(s3_queue_metadata|s3_queue_settings|t_merge_sibling_const_fold)$'
MERGE="merge(REGEXP('${DB_RE}'), '${TABLE_RE}')"
COND="column_b GLOBAL IN (SELECT 'c2' LIMIT 650, 280)"

# Every query below is SELECT DISTINCT: the two system siblings are shared, mutable server state
# whose row count depends on what else is running, and each of their rows renders the same value as
# the fixture row.

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_merge_sibling_const_fold (column_b String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sibling_const_fold SELECT 'c2';
"

# The Variant arm from the report, with and without prewhere: the folded column has to survive
# both, because either setting alone can move the filter and change which sibling folds.
for prewhere in 0 1; do
    ${CLICKHOUSE_CLIENT} -q "
    SET allow_suspicious_variant_types = 1, allow_suspicious_low_cardinality_types = 1,
        use_variant_as_common_type = 1,
        optimize_move_to_prewhere = ${prewhere}, query_plan_optimize_prewhere = ${prewhere};

    SELECT DISTINCT toString(multiIf(${COND}, 'yesyes', toLowCardinality(isNull('nononono')))) AS c
    FROM ${MERGE} WHERE c NOT IN ('_') ORDER BY c;
    "
done

# The same sibling divergence on other wrappers of the folded column.
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT toString(multiIf(${COND}, 'yesyes', 'no')) AS c
    FROM ${MERGE} WHERE c NOT IN ('_') ORDER BY c;"

# One branch NULL and the other foldable: the divergent column is Nullable, which is the shape
# the sibling Nullable(Nothing) reports in CIDB carry.
${CLICKHOUSE_CLIENT} --allow_suspicious_variant_types=1 --allow_suspicious_low_cardinality_types=1 \
    --use_variant_as_common_type=1 \
    -q "SELECT DISTINCT toString(multiIf(${COND}, NULL, toLowCardinality(isNull('nononono')))) AS c
    FROM ${MERGE} WHERE c IS NULL OR c IS NOT NULL ORDER BY c;"

${CLICKHOUSE_CLIENT} --allow_suspicious_low_cardinality_types=1 \
    -q "SELECT DISTINCT toString(multiIf(${COND}, toLowCardinality('yesyes'), toLowCardinality('no'))) AS c
    FROM ${MERGE} WHERE c NOT IN ('_') ORDER BY c;"

${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT toString(multiIf(${COND}, ['a'], ['b'])) AS c
    FROM ${MERGE} WHERE c != '' ORDER BY c;"

${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT toString(multiIf(${COND}, ('a', 1), ('b', 2))) AS c
    FROM ${MERGE} WHERE c != '' ORDER BY c;"

${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT toString(multiIf(${COND}, map('a', 1), map('b', 2))) AS c
    FROM ${MERGE} WHERE c != '' ORDER BY c;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_merge_sibling_const_fold;"
