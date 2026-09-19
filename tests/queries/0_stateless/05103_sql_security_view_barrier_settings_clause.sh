#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `SETTINGS` clause of the view's own query used to make `StorageView::canHideRows` fail closed,
# so a projection-only `DEFINER` / `NONE` view with a harmless execution setting lost inlining, the
# forwarded outer filter and PREWHERE just like a filtering view. Only a setting that can change the
# rows or the names the query produces may do that.

user="user05103_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF2
CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets VALUES ('a_${user}', 'visible'), ('z_someone_else', 'HIDDEN');

-- Two versions of one row: FINAL hides the older one.
CREATE TABLE $db.versions (owner String, secret String) ENGINE = ReplacingMergeTree ORDER BY owner;
SYSTEM STOP MERGES $db.versions;
INSERT INTO $db.versions VALUES ('a_${user}', 'visible'), ('z_someone_else', 'HIDDEN');
INSERT INTO $db.versions VALUES ('z_someone_else', 'replaced');

-- Execution tuning only: exposes exactly the rows of the source table.
CREATE VIEW $db.tuned_view DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets SETTINGS max_threads = 1, max_block_size = 65409;
CREATE VIEW $db.tuned_view_none SQL SECURITY NONE
AS SELECT owner, secret FROM $db.secrets SETTINGS max_threads = 1, max_block_size = 65409;
CREATE VIEW $db.tuned_view_invoker SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.secrets SETTINGS max_threads = 1, max_block_size = 65409;

-- The final setting rewrites the read as FINAL and hides the overwritten version of a row.
CREATE VIEW $db.final_view DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.versions SETTINGS final = 1;
CREATE VIEW $db.final_view_invoker SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.versions SETTINGS final = 1;

-- A read limit truncates the scan under a profile with read_overflow_mode = 'break'.
CREATE VIEW $db.read_limited_view DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets SETTINGS max_rows_to_read = 1000000;
CREATE VIEW $db.read_limited_view_invoker SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.secrets SETTINGS max_rows_to_read = 1000000;

-- Identifier binding: which column a name resolves to depends on this setting.
CREATE VIEW $db.rebinding_view DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets SETTINGS prefer_column_name_to_alias = 1;
CREATE VIEW $db.rebinding_view_invoker SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.secrets SETTINGS prefer_column_name_to_alias = 1;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.* TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF2

explain_client="${CLICKHOUSE_CLIENT} --user $user --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

echo "===== a view with only execution settings plans exactly like its INVOKER twin ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    for view in tuned_view tuned_view_none; do
        if diff -q \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$view WHERE secret = 'x'" 2>&1) \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.tuned_view_invoker WHERE secret = 'x'" 2>&1) > /dev/null
        then echo "same"; else echo "different"; fi
    done
done

echo "===== and it keeps PREWHERE ====="
${CLICKHOUSE_CLIENT} --user "$user" --enable_analyzer 1 --enable_parallel_replicas 0 \
    --optimize_move_to_prewhere 1 --query_plan_optimize_prewhere 1 --enable_multiple_prewhere_read_steps 1 --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.tuned_view WHERE secret = 'x'
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== a setting that can change the visible rows or the names still fails closed ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    for view in final_view read_limited_view rebinding_view; do
        if diff -q \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$view WHERE secret = 'x'" 2>&1) \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.${view}_invoker WHERE secret = 'x'" 2>&1) > /dev/null
        then echo "same"; else echo "different"; fi
    done
done

echo "===== the version hidden by the final setting is never observed ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    ${CLICKHOUSE_CLIENT} ${analyzer_settings} --user "$user" --query "SELECT count() FROM $db.final_view"
    ${CLICKHOUSE_CLIENT} ${analyzer_settings} --user "$user" --query \
        "SELECT * FROM $db.final_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
        grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo 1 || echo 0
done

echo "===== results through the tuned view are correct ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT secret FROM $db.tuned_view ORDER BY secret"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
