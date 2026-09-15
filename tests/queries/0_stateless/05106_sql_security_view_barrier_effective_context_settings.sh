#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proof that a `DEFINER` / `NONE` view hides nothing (`StorageView::canHideRows`) is run under
# the view's effective context, so a row-hiding setting inherited from the definer's profile counts
# exactly like a clause of the view's AST - it is invisible in the stored query. Its settings-only
# part (`StorageView::effectiveContextCanHideRows`) used to look at `limit` / `offset` and the two
# extra-filter settings only, so a definer profile `final = 1`, or a read limit with a non-throwing
# overflow mode, still passed as "projection only" and let both inliners and the `ORDER BY ...
# LIMIT` pushdown treat the view as transparent.
#
# A definer profile that hides no row at all (the first control) must keep the view transparent.

db=${CLICKHOUSE_DATABASE}
invoker="user05106_${CLICKHOUSE_DATABASE}_$RANDOM"
plain_definer="plain05106_${CLICKHOUSE_DATABASE}_$RANDOM"
final_definer="final05106_${CLICKHOUSE_DATABASE}_$RANDOM"
limit_definer="limit05106_${CLICKHOUSE_DATABASE}_$RANDOM"
ast_definer="ast05106_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.ecs_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.ecs_secrets VALUES ('a_owner', 'visible'), ('z_someone_else', 'other');

-- Two versions of one row: FINAL hides the older one.
CREATE TABLE $db.ecs_versions (owner String, secret String) ENGINE = ReplacingMergeTree ORDER BY owner;
SYSTEM STOP MERGES $db.ecs_versions;
INSERT INTO $db.ecs_versions VALUES ('a_owner', 'visible'), ('z_someone_else', 'HIDDEN');
INSERT INTO $db.ecs_versions VALUES ('z_someone_else', 'replaced');

CREATE USER $invoker;
-- Execution tuning only: the view exposes exactly the rows of the source table.
CREATE USER $plain_definer SETTINGS max_threads = 1, max_block_size = 65409;
-- Rewrites every source read as FINAL, hiding the overwritten version of a row.
CREATE USER $final_definer SETTINGS final = 1;
-- Truncates the scan instead of failing, so the view exposes a prefix of its rows.
CREATE USER $limit_definer SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break';
-- No settings at all: the twin below writes the same 'final' in the view's own AST instead.
CREATE USER $ast_definer;

GRANT SELECT ON $db.* TO $plain_definer, $final_definer, $limit_definer, $ast_definer, $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

CREATE VIEW $db.ecs_plain_view DEFINER = $plain_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.ecs_secrets;
CREATE VIEW $db.ecs_read_limited_view DEFINER = $limit_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.ecs_secrets;
CREATE VIEW $db.ecs_secrets_view_invoker SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.ecs_secrets;

CREATE VIEW $db.ecs_final_view DEFINER = $final_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.ecs_versions;
-- The same view with 'final' written in its own SETTINGS clause, where it has always failed
-- closed. The profile version must plan exactly like it.
CREATE VIEW $db.ecs_final_ast_view DEFINER = $ast_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.ecs_versions SETTINGS final = 1;
EOSQL

explain_client="${CLICKHOUSE_CLIENT} --user $invoker --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

function plans_the_same()
{
    if diff -q \
        <(${explain_client} $2 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$1 WHERE secret = 'x'" 2>&1) \
        <(${explain_client} $2 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.ecs_secrets_view_invoker WHERE secret = 'x'" 2>&1) > /dev/null
    then echo "same"; else echo "different"; fi
}

echo "===== a definer profile of pure execution tuning keeps the view transparent ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    plans_the_same ecs_plain_view "$analyzer_settings"
done

echo "===== a definer profile read limit that breaks instead of throwing fails closed ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    plans_the_same ecs_read_limited_view "$analyzer_settings"
done

echo "===== a definer profile final plans like the same final in the view's own SETTINGS ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    if diff -q \
        <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.ecs_final_view WHERE secret = 'x'" 2>&1) \
        <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.ecs_final_ast_view WHERE secret = 'x'" 2>&1) > /dev/null
    then echo "same"; else echo "different"; fi
done

echo "===== the version hidden by a definer profile final is never observed ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} ${analyzer_settings} --user "$invoker" --query "SELECT count() FROM $db.ecs_final_view"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} ${analyzer_settings} --user "$invoker" --short_circuit_function_evaluation disable --query \
        "SELECT * FROM $db.ecs_final_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
        grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo 1 || echo 0
done

echo "===== results through the transparent view are correct ====="
${CLICKHOUSE_CLIENT} --user "$invoker" --query "SELECT secret FROM $db.ecs_plain_view ORDER BY ALL"

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.ecs_plain_view, $db.ecs_read_limited_view, $db.ecs_secrets_view_invoker, $db.ecs_final_view, $db.ecs_final_ast_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $plain_definer, $final_definer, $limit_definer, $ast_definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.ecs_secrets, $db.ecs_versions"
