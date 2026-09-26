#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the encryption functions are not available in the fast test build
# Tag no-replicated-database: SQL SECURITY DEFINER views and users are set up per-test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EXPLAIN AST optimize = 1` inlines the body of a view the user may only SELECT from, and the dump
# prints every literal verbatim. The secret arguments must be hidden as `SHOW CREATE` hides them.
# The stateless test server keeps `display_secrets_in_show_and_select` off, so the gate always hides here.

user="user_05219_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}
key='Sixteen byte key'

${CLICKHOUSE_CLIENT} <<EOSQL
DROP TABLE IF EXISTS $db.private_plaintext;
CREATE TABLE $db.private_plaintext (secret String) ENGINE = Memory;
INSERT INTO $db.private_plaintext VALUES ('customer_token=prod_live_9fd17c2a');

CREATE VIEW $db.encrypted_view SQL SECURITY DEFINER AS
    SELECT hex(encrypt('aes-128-ecb', secret, '$key')) AS encrypted_secret
    FROM $db.private_plaintext;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.encrypted_view TO $user;
EOSQL

echo "-- the inlined view body hides the key"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST optimize = 1 SELECT * FROM $db.encrypted_view"

echo "-- the session setting alone does not disclose it"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST optimize = 1 SELECT * FROM $db.encrypted_view SETTINGS format_display_secrets_in_show_and_select = 1" | grep -F 'Literal'

echo "-- the graph dump hides it too"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST optimize = 1, graph = 1 SELECT * FROM $db.encrypted_view" | grep -o -E 'Literal [^"]*' | grep -F -e "$key" -e '[HIDDEN]'

echo "-- a secret typed into the explained query itself is hidden as well"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST SELECT encrypt('aes-128-ecb', 'plain', '$key', leftPad('iv', 16, '*'))"

echo "-- a positional secret written as a comparison collapses to one node, not to 'Function equals'"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST SELECT encrypt('aes-128-ecb', 'plain', 'k1' = 'k2')"

echo "-- a url the finder cannot read collapses to one node, not to 'Function concat'"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST SELECT * FROM url(concat('https://user:', 'p@host/f'))"

echo "-- the same url as a named override keeps its key, the value collapses"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST SELECT * FROM url(creds, url = concat('https://user:', 'p@host/f'))"

echo "-- a nested map keeps its keys and hides its values"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN AST SELECT * FROM url('http://x/f', headers('Authorization' = 'Bearer abc'))" | grep -F 'Literal'

echo "-- the view itself stays usable for the restricted user"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.encrypted_view"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.encrypted_view"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.private_plaintext"
