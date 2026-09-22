#!/usr/bin/env bash

# Tests that a table read by a mutation - on the right of `IN` or in a subquery - is reduced to a
# `SELECT` grant only when that grant is all that decides what the read returns. A background mutation
# reads for no user, so a row policy of the submitting user is not applied to it (the mutation would act
# on the rows the policy hides, and tell them apart), and an engine that checks the reading user's
# access to the objects it reads only when it is read (`Merge`, `View`, `MaterializedView`, `Buffer`,
# `Distributed`, a `system` table) passes those checks with full access. Both are refused.
# (https://github.com/ClickHouse/ClickHouse/issues/107588)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_05241"
policy_name="${CLICKHOUSE_DATABASE}_policy_05241"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;

CREATE TABLE rp_tab (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO rp_tab VALUES (1), (42);

-- The user may read this table, but a row policy hides the row 42 from them.
CREATE TABLE secret_tab (secret UInt32) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (1), (42);

-- A table the user may read with no policy on it.
CREATE TABLE plain_tab (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO plain_tab VALUES (1);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER DELETE, ALTER UPDATE, DELETE, SELECT ON $CLICKHOUSE_DATABASE.rp_tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.plain_tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.future_tab TO $user_name;

CREATE ROW POLICY $policy_name ON $CLICKHOUSE_DATABASE.secret_tab FOR SELECT USING secret < 10 TO $user_name;
-- Every other user reads the whole table (the test configuration refuses a user no policy of the table is for).
CREATE ROW POLICY ${policy_name}_others ON $CLICKHOUSE_DATABASE.secret_tab FOR SELECT USING 1 TO ALL EXCEPT $user_name;
-- A policy may be defined on a table that does not exist yet.
CREATE ROW POLICY ${policy_name}_future ON $CLICKHOUSE_DATABASE.future_tab FOR SELECT USING secret < 10 TO $user_name;
"

# Prints the error a query is refused with (`OK` when it is not). Runs as the test's own user, on whom
# no policy applies and who has every grant, when the second argument is `admin`. The accepted
# mutations keep their predicates false, so nothing is ever mutated and nothing has to be waited for.
function check_refusal()
{
    local client="$CLICKHOUSE_CLIENT"
    [ "${2:-}" = "admin" ] || client="$client --user $user_name --password password"
    local output
    output=$($client -q "$1" 2>&1) && { echo "OK"; return; }
    echo "$output" | grep -oE "\([A-Z_]+\)$" | head -1
}

echo "-- The row policy hides a row from the user's own SELECT"
$CLICKHOUSE_CLIENT --user "$user_name" --password password -q "SELECT count() FROM secret_tab"

echo "-- A table on which the user has a row policy cannot be read by the user's mutation, on every entry point"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM secret_tab)"
check_refusal "ALTER TABLE rp_tab UPDATE id = id WHERE id IN (SELECT secret FROM secret_tab)"
check_refusal "ALTER TABLE rp_tab UPDATE id = (SELECT max(secret) FROM secret_tab) WHERE 0"
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT secret FROM secret_tab)"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN secret_tab"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM secret_tab) SETTINGS validate_mutation_query = 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM view(SELECT secret FROM secret_tab)) SETTINGS validate_mutation_query = 0"
echo "-- ... even when the table does not exist yet and validation is deferred"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM future_tab) SETTINGS validate_mutation_query = 0"
echo "-- ... and when the table is one of those a merge table function matches (which needs SELECT on the whole database)"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user_name"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$'))"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) AND 0" admin

echo "-- A table with no row policy for the user is read under its SELECT grant"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT id FROM plain_tab) AND 0"
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT id FROM plain_tab) AND 0"
echo "-- ... and so is the restricted table for a user the policy does not apply to"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0" admin
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT secret FROM secret_tab) AND 0" admin

echo "-- Once the policy is dropped the same mutations are accepted"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY $policy_name, ${policy_name}_others ON $CLICKHOUSE_DATABASE.secret_tab"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0"
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT secret FROM secret_tab) AND 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN secret_tab AND 0"

# The engines below read other objects of the server and check the reading user's access to them, or
# apply their row policies, only when they are read.
$CLICKHOUSE_CLIENT -q "
CREATE VIEW secret_view AS SELECT secret FROM secret_tab;
CREATE MATERIALIZED VIEW secret_mv ENGINE = MergeTree ORDER BY secret AS SELECT secret FROM secret_tab;
CREATE TABLE secret_merge (secret UInt32) ENGINE = Merge('$CLICKHOUSE_DATABASE', '^secret_tab\$');
CREATE TABLE secret_buffer (secret UInt32) ENGINE = Buffer('$CLICKHOUSE_DATABASE', 'secret_tab', 1, 10, 100, 10000, 1000000, 10000000, 100000000);
CREATE TABLE secret_dist (secret UInt32) ENGINE = Distributed(test_shard_localhost, '$CLICKHOUSE_DATABASE', 'secret_tab');

-- The engines below read their own data, and nothing else.
CREATE TABLE mem_tab (id UInt32) ENGINE = Memory;
CREATE TABLE log_tab (id UInt32) ENGINE = StripeLog;
CREATE DICTIONARY plain_dict (id UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'plain_tab' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);

GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user_name;
GRANT SELECT ON system.* TO $user_name;
"

echo "-- A table whose engine checks the reading user's access only when it is read cannot be read by a mutation, for any user"
for table in secret_view secret_mv secret_merge secret_buffer secret_dist
do
    check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM $table)"
    check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM $table)" admin
done
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT secret FROM secret_view)"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN secret_view"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT 1 FROM system.users)"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT 1 FROM system.users)" admin
echo "-- ... also when a merge table function matches it"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_view\$'))"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_view\$'))" admin

echo "-- A table whose engine reads its own data is read under its SELECT grant"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT id FROM mem_tab) AND 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT id FROM log_tab) AND 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT id FROM plain_dict) AND 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT number FROM system.numbers LIMIT 1) AND 0"
check_refusal "ALTER TABLE rp_tab DELETE WHERE id IN (SELECT dummy FROM system.one) AND 0"
check_refusal "DELETE FROM rp_tab WHERE id IN (SELECT id FROM mem_tab) AND 0"

echo "-- Nothing was mutated"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM rp_tab"

$CLICKHOUSE_CLIENT -q "
DROP ROW POLICY IF EXISTS ${policy_name}_others ON $CLICKHOUSE_DATABASE.secret_tab;
DROP ROW POLICY IF EXISTS ${policy_name}_future ON $CLICKHOUSE_DATABASE.future_tab;
DROP USER IF EXISTS $user_name;
"
