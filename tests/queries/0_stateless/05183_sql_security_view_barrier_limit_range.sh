#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `LIMIT n AFTER expr UNTIL expr` selects a range of the sorted result and hides every row outside
# of it, exactly like `LIMIT` / `OFFSET` do. `StorageView::canHideRows` used to look only at
# `limitLength` / `limitOffset`, so a `SQL SECURITY DEFINER` view whose only row-hiding carrier is
# that clause was classified as projection-only: it kept being inlined into the invoker's query
# (the legacy `InterpreterSelectQuery` path and `QueryAnalyzer::inlineViewSubqueryIfNeeded`) and
# `readImpl` built no barrier step, so the invoker's own expressions could be evaluated on the
# rows the range hides.
#
# The plan half is the discriminating oracle: the view that hides rows keeps the sealing
# `Convert VIEW subquery result to VIEW table structure` step on every path, while the twin view
# with the same body minus the range - a genuinely projection-only view - is still inlined and has
# no such step.

db=${CLICKHOUSE_DATABASE}
invoker="user05183_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05183_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.security_view_range_source (k UInt64, secret String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.security_view_range_source SELECT number, concat('secret-', toString(number)) FROM numbers(8);

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.security_view_range_source TO $definer;

-- The range is the only row-hiding construct: rows k = 3, 4 are visible, everything else is not.
CREATE VIEW $db.security_view_range
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT k, secret FROM $db.security_view_range_source ORDER BY k LIMIT 2 AFTER k >= 3 UNTIL k >= 5;
GRANT SELECT ON $db.security_view_range TO $invoker;

-- The twin without the range hides nothing, so it must keep being inlined.
CREATE VIEW $db.security_view_plain
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT k, secret FROM $db.security_view_range_source ORDER BY k;
GRANT SELECT ON $db.security_view_plain TO $invoker;
EOF

# The harness randomizes settings, and parallel replicas or in-order reading reshape the subplan.
PIN_SETTINGS="--enable_parallel_replicas 0 --extremes 0 --exact_rows_before_limit 0 \
    --prefer_column_name_to_alias 0 --optimize_read_in_order 0"

echo "===== the view exposes only the rows of the range ====="
for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PIN_SETTINGS $settings --user "$invoker" --query \
        "SELECT k FROM $db.security_view_range ORDER BY k" | tr '\n' ' '
    echo
done

echo "===== an invoker expression never runs on a row outside of the range ====="
for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PIN_SETTINGS $settings --user "$invoker" --query \
        "SELECT count() FROM $db.security_view_range WHERE NOT throwIf(secret = 'secret-7', 'DISCLOSED')" 2>&1 \
        | grep -q 'DISCLOSED' && echo "disclosed" || echo "not disclosed"
done

echo "===== the invoker's WHERE never merges into the range view's subplan ====="
# Two numbers per view: how many plan steps carry the view's sealing
# `Convert VIEW subquery result to VIEW table structure` description, and how many of those also
# carry the invoker's `WHERE` - a merged step means the invoker's predicate entered the view's
# subplan, which is exactly what the barrier forbids. The range view keeps the seal with nothing
# merged into it on every path; the projection-only twin, which hides nothing, either merges the
# predicate into the conversion or (with `analyzer_inline_views = 1`) is inlined away entirely.
for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    for view in security_view_range security_view_plain; do
        # shellcheck disable=SC2086
        plan=$(${CLICKHOUSE_CLIENT} $PIN_SETTINGS $settings --user "$invoker" --query \
            "EXPLAIN compact = 0 SELECT k FROM $db.$view WHERE secret != 'x'" \
            | grep "Convert VIEW subquery result")
        echo -n "$(echo -n "$plan" | grep -c "Convert VIEW subquery result")/$(echo -n "$plan" | grep -c "WHERE") "
    done
    echo
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_range"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_plain"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.security_view_range_source"
