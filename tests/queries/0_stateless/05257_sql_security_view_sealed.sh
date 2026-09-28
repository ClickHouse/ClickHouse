#!/usr/bin/env bash

# A view with `SQL SECURITY DEFINER` or `NONE` that hides rows is read through an opaque step,
# so the invoker's expressions and predicates never see the rows the view drops.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05257_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05257_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user, $definer;
CREATE USER $user;
CREATE USER $definer;

CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');

CREATE VIEW $db.definer_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.none_view SQL SECURITY NONE AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.invoker_view SQL SECURITY INVOKER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.projection_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets;

-- A plain projection still hides rows if the definer has a row policy on the table.
CREATE TABLE $db.policy_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.policy_secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');
CREATE ROW POLICY policy05257 ON $db.policy_secrets USING owner = 'alice' TO $definer;
GRANT SELECT ON $db.policy_secrets TO $definer;
CREATE VIEW $db.policy_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.policy_secrets;

-- So does a plain projection if the invoker has a row policy on the view itself.
CREATE VIEW $db.view_policy_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets;
CREATE ROW POLICY view_policy05257 ON $db.view_policy_view USING owner = 'alice' TO $user;

GRANT SELECT ON $db.definer_view TO $user;
GRANT SELECT ON $db.none_view TO $user;
GRANT SELECT ON $db.policy_view TO $user;
GRANT SELECT ON $db.view_policy_view TO $user;
EOF

echo "--- an outer expression is not evaluated on the hidden rows"
for view in definer_view none_view policy_view view_policy_view; do
    for inline in 0 1; do
        ${CLICKHOUSE_CLIENT} --user "$user" --analyzer_inline_views "$inline" --query "
            SELECT secret FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
    done
done

echo "--- nor on a remote server"
for serialize in 0 1; do
    ${CLICKHOUSE_CLIENT} --serialize_query_plan "$serialize" --query "
        SELECT secret FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', 'definer_view', '$user', '')
        WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
done

echo "--- an outer predicate does not skip data by the values of the hidden rows"
# The table is sorted by `secret`, so a predicate on it would skip granules by the primary key.
for view in definer_view policy_view view_policy_view; do
    for secret in HIDDEN nonexistent; do
        ${CLICKHOUSE_CLIENT} --user "$user" --use_query_condition_cache 0 --query_id "05257_${CLICKHOUSE_DATABASE}_${view}_$secret" --query "
            SELECT count() FROM $db.$view WHERE secret = '$secret'"
    done
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT query_id LIKE '%policy_view%', read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '05257_${CLICKHOUSE_DATABASE}_%'
    ORDER BY query_id"

echo "--- an outer ORDER BY ... LIMIT is not pushed into the view to stop reading early"
# Reading `secrets` in reverse order of `secret` would stop at the visible row before reaching the hidden one.
for view in definer_view none_view; do
    ${CLICKHOUSE_CLIENT} --user "$user" --use_query_condition_cache 0 --max_threads 1 --max_block_size 1 --query_id "05257order_${CLICKHOUSE_DATABASE}_${view}" --query "
        SELECT secret FROM $db.$view ORDER BY secret DESC LIMIT 1"
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '05257order_${CLICKHOUSE_DATABASE}_%'
    ORDER BY query_id"

echo "--- the view is still queried as usual"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT owner, secret FROM $db.definer_view ORDER BY secret LIMIT 1;
    SELECT count(), max(secret) FROM $db.definer_view GROUP BY owner;
    SELECT secret FROM $db.definer_view WHERE secret LIKE 'vis%';"

echo "--- a plan fragment that reads the view can be cloned"
# Without the in-memory buffer, the input of a correlated subquery is duplicated by cloning its plan.
${CLICKHOUSE_CLIENT} --query "
    SELECT secret FROM $db.definer_view AS v WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number = length(v.secret))
    SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 0"

echo "--- only a view that runs with other privileges and can hide rows is sealed"
for view in definer_view none_view invoker_view projection_view policy_view; do
    echo -n "$view: "
    ${CLICKHOUSE_CLIENT} --query "SELECT countIf(explain LIKE '%ReadFromSealedView%') FROM (EXPLAIN SELECT * FROM $db.$view WHERE secret = 'x')"
done

echo -n "view_policy_view for the user with the policy: "
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN SELECT * FROM $db.view_policy_view WHERE secret = 'x'" | grep -c ReadFromSealedView

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.policy_view; DROP ROW POLICY policy05257 ON $db.policy_secrets; DROP ROW POLICY view_policy05257 ON $db.view_policy_view; DROP USER $user, $definer"
