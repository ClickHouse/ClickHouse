#!/usr/bin/env bash

# A view with `SQL SECURITY DEFINER` or `NONE` that hides rows is read through an opaque step,
# so the invoker's expressions and predicates never see the rows the view drops.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05257_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;

CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');

CREATE VIEW $db.definer_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.none_view SQL SECURITY NONE AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.invoker_view SQL SECURITY INVOKER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.projection_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets;

GRANT SELECT ON $db.definer_view TO $user;
GRANT SELECT ON $db.none_view TO $user;
EOF

echo "--- an outer expression is not evaluated on the hidden rows"
for view in definer_view none_view; do
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
for secret in HIDDEN nonexistent; do
    ${CLICKHOUSE_CLIENT} --user "$user" --use_query_condition_cache 0 --query_id "05257_${CLICKHOUSE_DATABASE}_$secret" --query "
        SELECT count() FROM $db.definer_view WHERE secret = '$secret'"
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '05257_${CLICKHOUSE_DATABASE}_%'
    ORDER BY query_id"

echo "--- the view is still queried as usual"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT owner, secret FROM $db.definer_view ORDER BY secret LIMIT 1;
    SELECT count(), max(secret) FROM $db.definer_view GROUP BY owner;
    SELECT secret FROM $db.definer_view WHERE secret LIKE 'vis%';"

echo "--- only a view that runs with other privileges and can hide rows is sealed"
for view in definer_view none_view invoker_view projection_view; do
    echo -n "$view: "
    ${CLICKHOUSE_CLIENT} --query "SELECT countIf(explain LIKE '%ReadFromSealedView%') FROM (EXPLAIN SELECT * FROM $db.$view WHERE secret = 'x')"
done

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
