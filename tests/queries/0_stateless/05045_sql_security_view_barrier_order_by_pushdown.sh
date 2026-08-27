#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `pushOrderByIntoView` injects the invoker's `ORDER BY ... LIMIT` into the view's inner query.
# The definer profile's `additional_result_filter` grows a filter step on top of the inner query's
# result, after the inner plan is built, so an injected inner `LIMIT` used to truncate the rows
# before that filter dropped its share: with rows `k = 1, 2` and a definer filter `k = 2`, the
# inner query kept `k = 1` and the post-filter then removed it, so `SELECT k FROM v ORDER BY k
# LIMIT 1` returned no row instead of `2`. The pushdown now fails closed on an effective-context
# `additional_result_filter` / `additional_table_filters`.
#
# The plan half pins that the guard, and not something else, blocks the pushdown: the twin view
# whose definer has no such filter still gets the inner `Sorting` step below the sealing
# `Convert VIEW subquery result` expression, the filtered one does not.

db=${CLICKHOUSE_DATABASE}
invoker="user05045_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05045_${CLICKHOUSE_DATABASE}_$RANDOM"
definer_plain="definerplain05045_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.security_view_obp_source (k UInt64, owner String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.security_view_obp_source VALUES (1, 'bob'), (2, 'alice');

CREATE USER $invoker;
CREATE USER $definer SETTINGS additional_result_filter = 'k = 2';
CREATE USER $definer_plain;
GRANT SELECT ON $db.security_view_obp_source TO $definer;
GRANT SELECT ON $db.security_view_obp_source TO $definer_plain;

CREATE VIEW $db.security_view_obp_filtered
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT k, owner FROM $db.security_view_obp_source;
GRANT SELECT ON $db.security_view_obp_filtered TO $invoker;

CREATE VIEW $db.security_view_obp_plain
DEFINER = $definer_plain SQL SECURITY DEFINER
AS SELECT k, owner FROM $db.security_view_obp_source;
GRANT SELECT ON $db.security_view_obp_plain TO $invoker;
EOF

# The settings the plan shape and the pushdown decision depend on are pinned, because the harness
# randomizes settings: `extremes`, `exact_rows_before_limit` and `prefer_column_name_to_alias`
# disable the pushdown, parallel replicas turn the subplan into a union, and in-order reading
# reshapes the inner sort.
PIN_SETTINGS="--enable_parallel_replicas 0 --extremes 0 --exact_rows_before_limit 0 \
    --prefer_column_name_to_alias 0 --optimize_read_in_order 0"

for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PIN_SETTINGS $settings --user "$invoker" --query \
        "SELECT k FROM $db.security_view_obp_filtered ORDER BY k LIMIT 1"
done

for view in security_view_obp_plain security_view_obp_filtered; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PIN_SETTINGS --enable_analyzer 1 --user "$invoker" --query \
        "EXPLAIN compact = 0 SELECT k FROM $db.$view ORDER BY k LIMIT 1" \
        | awk '/Convert VIEW subquery result/{f=1} f' | grep -c "Sorting"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_obp_filtered"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_obp_plain"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer_plain"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.security_view_obp_source"
