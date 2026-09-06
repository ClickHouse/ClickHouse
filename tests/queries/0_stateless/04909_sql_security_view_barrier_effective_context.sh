#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
invoker="user04909_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer04909_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.security_view_context_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.security_view_context_secrets VALUES ('${invoker}', 'visible'), ('someone_else', 'HIDDEN');

CREATE USER $invoker;
CREATE USER $definer SETTINGS limit = 1;
GRANT SELECT ON $db.security_view_context_secrets TO $definer;

CREATE VIEW $db.security_view_context_limit
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT * FROM $db.security_view_context_secrets;
GRANT SELECT ON $db.security_view_context_limit TO $invoker;

CREATE VIEW $db.security_view_context_policy
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT * FROM $db.security_view_context_secrets;
GRANT SELECT ON $db.security_view_context_policy TO $invoker;

-- The policy must be created after the views: with the legacy analyzer, CREATE VIEW resolves the
-- view's sample block as the creating user, and a table that has row policies for other users
-- only makes that resolution fail with ACCESS_DENIED. The policy applies at query time either way.
CREATE ROW POLICY ${definer}_source_policy ON $db.security_view_context_secrets
FOR SELECT USING owner = '${invoker}' TO $definer;
EOF

for view in security_view_context_limit security_view_context_policy; do
    for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
        # shellcheck disable=SC2086
        ${CLICKHOUSE_CLIENT} $settings --user "$invoker" --query \
            "SELECT * FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 | grep -c -F LEAKED
    done
done

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${definer}_source_policy ON $db.security_view_context_secrets"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_context_limit"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_context_policy"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.security_view_context_secrets"
