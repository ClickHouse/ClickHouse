#!/usr/bin/env bash

# Tests that a mutation reading through a table function - on the right of `IN` or in the `FROM` of a
# subquery - requires the access a call of that function requires, and that the requirement does not
# depend on `validate_mutation_query`. A table function names the data it reads in its arguments
# instead of naming an object to grant on, and the instance of it that checks the source access is
# built only when the set is built - for a mutation, in the background, under full access
# (https://github.com/ClickHouse/ClickHouse/issues/107588).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_05239"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tf_tab, secret_tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tf_tab (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tf_tab VALUES (1);

-- The table the user has no access to.
CREATE TABLE secret_tab (secret UInt32, payload String) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (42, 'TOP-SECRET');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER DELETE, DELETE, SELECT ON $CLICKHOUSE_DATABASE.tf_tab TO $user_name;
"

# Prints whether access control rejected the query. The mutations are never waited for: one that names
# a file that does not exist, or a function the background has no user for, fails for ever in the
# background, and a waited mutation on the same table would then inherit that failure. Only the
# access decision is the point.
function check_access()
{
    if $CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1 | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "NOT_DENIED"
    fi
}

# Prints the error a query is refused with (`OK` when it is not), for the cases refused for a reason
# other than access. Runs as the test's own user when the second argument is `admin`.
function check_refusal()
{
    local client="$CLICKHOUSE_CLIENT"
    [ "${2:-}" = "admin" ] || client="$client --user $user_name --password password"
    local output
    output=$($client -q "$1" 2>&1) && { echo "OK"; return; }
    echo "$output" | grep -oE "\([A-Z_]+\)$" | head -1
}

# Every case below is run with validation off, which is what makes the read invisible to the
# submission-time validation; the requirement has to come from the access check itself.
off="validate_mutation_query = 0"

echo "-- A table function read requires the source access of its call, on the right of IN and in a FROM"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN file('05239_no_such_file.tsv', 'TSV', 'id UInt32') SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM file('05239_no_such_file.tsv', 'TSV', 'id UInt32')) SETTINGS $off"
echo "-- A read-only table function with no source of its own needs no grant, as in a plain SELECT"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN numbers(2) AND 0 SETTINGS $off"

# `view(SELECT ...)` carries its query as a bare argument rather than as a parenthesised subquery,
# and the tables that query reads are read all the same when the mutation runs.
echo "-- A table function that takes a query reads the tables of that query"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM view(SELECT secret FROM secret_tab)) SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM view(SELECT secret FROM view(SELECT secret FROM secret_tab))) SETTINGS $off"
check_access "DELETE FROM tf_tab WHERE id IN (SELECT secret FROM view(SELECT secret FROM secret_tab)) SETTINGS $off"

# What `merge` reads is the tables of the database its name pattern matches, and `SELECT` on them is
# checked only when they are read - for a mutation, in the background, under full access. The tables
# are not known when the mutation is submitted (the pattern is not run against the catalog), so the
# read is taken for a read of every table of the database named by a literal, and of every table
# there is when the database is not a literal.
echo "-- A table function over the tables of the server requires SELECT on every table it can name"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN merge('$CLICKHOUSE_DATABASE', '^secret_tab\$') SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) SETTINGS $off"
check_access "DELETE FROM tf_tab WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM merge(currentDatabase(), '^secret_tab\$')) SETTINGS $off"
echo "-- SELECT on the very table it matches is not enough: the tables matched are not known when the mutation is checked"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) SETTINGS $off"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON $CLICKHOUSE_DATABASE.secret_tab FROM $user_name"

# What `viewIfPermitted` or `mergeTreeTextIndex` reads is decided by the grants of the user it runs
# for, and a mutation runs it later, in the background, for no user at all - so there is no grant to
# require at submission that would keep the meaning it was checked with, and the function is refused
# in a mutation for every user, as it is in a persisted `CREATE TABLE ... AS`.
echo "-- A table function whose reads depend on the current user's grants cannot be stored in a mutation, for any user"
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM viewIfPermitted(SELECT id FROM tf_tab ELSE null('id UInt32'))) SETTINGS $off"
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM view(SELECT id FROM viewIfPermitted(SELECT id FROM tf_tab ELSE null('id UInt32')))) SETTINGS $off"
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT 1 FROM mergeTreeTextIndex('$CLICKHOUSE_DATABASE', 'tf_tab', 'idx')) SETTINGS $off"
check_refusal "DELETE FROM tf_tab WHERE id IN (SELECT id FROM viewIfPermitted(SELECT id FROM tf_tab ELSE null('id UInt32'))) SETTINGS $off"
echo "-- Also with validation on, and for a user with every grant"
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM viewIfPermitted(SELECT id FROM tf_tab ELSE null('id UInt32')))" admin
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT 1 FROM mergeTreeTextIndex('$CLICKHOUSE_DATABASE', 'tf_tab', 'idx')) SETTINGS $off" admin
echo "-- The same query as a plain view is not refused for that user"
check_refusal "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM view(SELECT id FROM tf_tab WHERE 0)) SETTINGS $off" admin

echo "-- With the source grant the same mutations are accepted"
# A table function that is not read-only needs `CREATE TEMPORARY TABLE` as well, in a mutation as in
# a plain `SELECT`.
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE, CREATE TEMPORARY TABLE ON *.* TO $user_name"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN file('05239_no_such_file.tsv', 'TSV', 'id UInt32') AND 0 SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT id FROM file('05239_no_such_file.tsv', 'TSV', 'id UInt32')) AND 0 SETTINGS $off"
echo "-- With SELECT on the table the query of the table function reads, the same mutations are accepted"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM view(SELECT secret FROM secret_tab)) AND 0 SETTINGS $off"
check_access "DELETE FROM tf_tab WHERE id IN (SELECT secret FROM view(SELECT secret FROM secret_tab)) AND 0 SETTINGS $off"
echo "-- With SELECT on every table of the database, merge over a literal database is accepted"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user_name"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN merge('$CLICKHOUSE_DATABASE', '^secret_tab\$') AND 0 SETTINGS $off"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) AND 0 SETTINGS $off"
check_access "DELETE FROM tf_tab WHERE id IN (SELECT secret FROM merge('$CLICKHOUSE_DATABASE', '^secret_tab\$')) AND 0 SETTINGS $off"
echo "-- ... but not one whose database is not a literal, which can name a table of any database"
check_access "ALTER TABLE tf_tab DELETE WHERE id IN (SELECT secret FROM merge(currentDatabase(), '^secret_tab\$')) AND 0 SETTINGS $off"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tf_tab, secret_tab;
DROP USER IF EXISTS $user_name;
"
