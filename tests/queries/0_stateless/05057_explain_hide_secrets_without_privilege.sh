#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the encryption functions are not available in the fast test build
# Tag no-replicated-database: SQL SECURITY DEFINER views and users are set up per-test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user with only SELECT on a SQL SECURITY DEFINER view must not be able to read the
# encryption key of the view definition through any flavour of EXPLAIN, no matter which
# value of `format_display_secrets_in_show_and_select` it sets (the stateless test server
# does not enable the `display_secrets_in_show_and_select` server setting, so the full
# gate always hides secrets here).

user="user_05055_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}
key='Sixteen byte key'

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS $db.encrypted_view;"
${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS $db.sorted_view;"
${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS $db.nested_const_view;"
${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS $db.where_const_view;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.private_plaintext;"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.private_plaintext (secret String) ENGINE = Memory;
INSERT INTO $db.private_plaintext VALUES ('customer_token=prod_live_9fd17c2a');

CREATE VIEW $db.encrypted_view SQL SECURITY DEFINER AS
    SELECT hex(encrypt('aes-128-ecb', secret, '$key')) AS encrypted_secret
    FROM $db.private_plaintext;

CREATE VIEW $db.sorted_view SQL SECURITY DEFINER AS
    SELECT secret FROM $db.private_plaintext
    ORDER BY hex(encrypt('aes-128-ecb', secret, '$key'));

CREATE VIEW $db.nested_const_view SQL SECURITY DEFINER AS
    SELECT concat(secret, hex(encrypt('aes-128-ecb', 'static', 'Sixteen byte key'))) AS tagged
    FROM $db.private_plaintext;

CREATE VIEW $db.where_const_view SQL SECURITY DEFINER AS
    SELECT secret
    FROM $db.private_plaintext
    WHERE secret != hex(encrypt('aes-128-ecb', 'static', 'Sixteen byte key'));

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.encrypted_view TO $user;
GRANT SELECT ON $db.sorted_view TO $user;
GRANT SELECT ON $db.nested_const_view TO $user;
GRANT SELECT ON $db.where_const_view TO $user;
EOF

# The analyzer, the session setting alone must not disclose secrets
new_settings="SETTINGS enable_analyzer = 1, format_display_secrets_in_show_and_select = 1"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN actions = 1 SELECT * FROM $db.encrypted_view $new_settings"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN actions = 1 SELECT * FROM $db.encrypted_view $new_settings, explain_query_plan_default = 'legacy'"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN actions = 1 SELECT * FROM $db.nested_const_view $new_settings, explain_query_plan_default = 'legacy'"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN actions = 1 SELECT * FROM $db.where_const_view $new_settings, explain_query_plan_default = 'legacy'"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN json = 1, actions = 1 SELECT * FROM $db.encrypted_view $new_settings"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN header = 1 SELECT * FROM $db.encrypted_view $new_settings"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN QUERY TREE SELECT * FROM $db.encrypted_view $new_settings"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN SYNTAX SELECT * FROM $db.encrypted_view $new_settings"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN PIPELINE header = 1 SELECT * FROM $db.sorted_view $new_settings"

function expect_refused()
{
      ${CLICKHOUSE_CLIENT} --user "$user" --query "$1" 2>&1 | grep -o 'ACCESS_DENIED'
}


# old analyzer never masks, so EXPLAIN of a secret-bearing plan is refused
old_settings="SETTINGS enable_analyzer = 0"
expect_refused "EXPLAIN actions = 1 SELECT * FROM $db.encrypted_view $old_settings, explain_query_plan_default = 'legacy'"
expect_refused "EXPLAIN actions = 1 SELECT * FROM $db.nested_const_view $old_settings, explain_query_plan_default = 'legacy'"
expect_refused "EXPLAIN actions = 1 SELECT * FROM $db.where_const_view $old_settings, explain_query_plan_default = 'legacy'"
expect_refused "EXPLAIN header = 1 SELECT * FROM $db.encrypted_view $old_settings"
expect_refused "EXPLAIN ANALYZE actions = 1 SELECT * FROM $db.encrypted_view $old_settings"
expect_refused "EXPLAIN ANALYZE actions = 1 SELECT * FROM $db.nested_const_view $old_settings, explain_query_plan_default = 'legacy'"
expect_refused "EXPLAIN PIPELINE header = 1 SELECT * FROM $db.sorted_view $old_settings"
expect_refused "EXPLAIN PIPELINE SELECT * FROM $db.sorted_view $old_settings"

echo "-- the view itself stays usable for the restricted user"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.encrypted_view"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
