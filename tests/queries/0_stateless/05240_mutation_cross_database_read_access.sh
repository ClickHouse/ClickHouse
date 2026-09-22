#!/usr/bin/env bash

# Tests that what a mutation reads by an unqualified name - a table on the right of `IN`, a table of a
# subquery, the object of `dictGet` or `joinGet` - is checked against, and bound to, the database of
# the mutated table and not the session's current database, on every entry point and with
# `validate_mutation_query` off. A background mutation has no current database and runs under full
# access, so a check against the session's database could be satisfied by a grant on a table of the
# same name there, or the stored mutation could read another database's object than the one checked
# (https://github.com/ClickHouse/ClickHouse/issues/107588).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_05240"
# A second database, to be the session's current database while the mutated table is in another.
other_db="${CLICKHOUSE_DATABASE}_other_05240"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, xdb_tab, secret_tab, secret_set, join_tab, dict_src;
DROP DICTIONARY IF EXISTS dict;
DROP USER IF EXISTS $user_name;
DROP DATABASE IF EXISTS $other_db;
CREATE DATABASE $other_db;

CREATE TABLE tab (id UInt32, name String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a'), (42, 'b');

-- The tables, set, dictionary and Join table the user has no access to.
CREATE TABLE secret_tab (secret UInt32, payload String) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (42, 'TOP-SECRET');
CREATE TABLE secret_set (secret UInt32) ENGINE = Set;
INSERT INTO secret_set VALUES (42);
CREATE TABLE join_tab (id UInt32, payload String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO join_tab VALUES (1, 'joined'), (2, 'joined');
CREATE TABLE dict_src (key UInt64, payload String) ENGINE = MergeTree ORDER BY key;
INSERT INTO dict_src VALUES (1, 'from-dict'), (2, 'from-dict');
CREATE DICTIONARY dict (key UInt64, payload String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER UPDATE, ALTER DELETE, UPDATE, DELETE, SELECT(id, name) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
-- The user may read tables of these names in the other database - where none of them exists.
GRANT SELECT ON $other_db.secret_tab TO $user_name;
GRANT SELECT ON $other_db.secret_set TO $user_name;
GRANT dictGet ON $other_db.dict TO $user_name;
GRANT SELECT ON $other_db.join_tab TO $user_name;
"

# Runs a query as the user, from a session whose current database is the other database.
function check_access()
{
    local client="${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$other_db}"
    local output
    output=$($client --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

# Every case below is run with validation off, which is what makes the read invisible to the
# submission-time validation; the requirement has to come from the access check itself.
off="validate_mutation_query = 0, mutations_sync = 2"

# The mutation expression is qualified with the database of the mutated table before it is stored,
# so an unqualified table in it is read from that database - not from the session's current one,
# where the user may read a table of the same name (here, one that does not even exist).
echo "-- An unqualified table is read from the mutated table's database, not the session's"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab DELETE WHERE id IN secret_set SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab DELETE WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS $off"
check_access "DELETE FROM $CLICKHOUSE_DATABASE.tab WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"
check_access "UPDATE $CLICKHOUSE_DATABASE.tab SET name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS $off, enable_lightweight_update = 1"

# `dictGet` and `joinGet` name their object by an unqualified name as well, and the same visitor
# qualifies that name with the database of the mutated table, so the object read is that database's
# one - not the same-named one the session's current database may hold.
echo "-- An unqualified dictGet / joinGet object is read from the mutated table's database too"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = dictGet('dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = joinGet('join_tab', 'payload', id) WHERE 0 SETTINGS $off"

# The stored mutation is bound to the very object the check required. The session's database now
# holds a dictionary and a Join table of the same names with other values, and the user holds the
# grants on the mutated table's database's ones: every mutation path reads those, and none of the
# session's - which the user may read too, so a mismatch would show in the values, not as a denial.
# The table is its own, so that a mutation left behind cannot fail the other cases.
echo "-- ... and the stored mutation reads that database's object, not the session's same-named one"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE $other_db.dict_src (key UInt64, payload String) ENGINE = MergeTree ORDER BY key;
INSERT INTO $other_db.dict_src VALUES (1, 'from-other-dict'), (2, 'from-other-dict');
CREATE DICTIONARY $other_db.dict (key UInt64, payload String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '$other_db')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE $other_db.join_tab (id UInt32, payload String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO $other_db.join_tab VALUES (1, 'joined-other'), (2, 'joined-other');
CREATE TABLE xdb_tab (id UInt32, name String, name2 String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO xdb_tab VALUES (1, '', ''), (2, '', ''), (3, '', '');
GRANT ALTER UPDATE, ALTER DELETE, UPDATE, DELETE, SELECT ON $CLICKHOUSE_DATABASE.xdb_tab TO $user_name;
GRANT dictGet ON $CLICKHOUSE_DATABASE.dict TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.join_tab TO $user_name;
"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.xdb_tab UPDATE name = dictGet('dict', 'payload', toUInt64(id)), name2 = joinGet('join_tab', 'payload', id) WHERE id = 1 SETTINGS $off"
check_access "UPDATE $CLICKHOUSE_DATABASE.xdb_tab SET name = dictGet('dict', 'payload', toUInt64(id)), name2 = joinGet('join_tab', 'payload', id) WHERE id = 2 SETTINGS $off, enable_lightweight_update = 1"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.xdb_tab DELETE WHERE dictGet('dict', 'payload', toUInt64(id)) = 'from-other-dict' OR joinGet('join_tab', 'payload', id) = 'joined-other' SETTINGS $off"
check_access "DELETE FROM $CLICKHOUSE_DATABASE.xdb_tab WHERE dictGet('dict', 'payload', toUInt64(id)) = 'from-other-dict' OR joinGet('join_tab', 'payload', id) = 'joined-other' SETTINGS $off"
$CLICKHOUSE_CLIENT -q "SELECT id, name, name2 FROM xdb_tab ORDER BY id"

$CLICKHOUSE_CLIENT -q "
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_set TO $user_name;
"

echo "-- With the grants on the mutated table's database the mutations run, so they do read that database"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab DELETE WHERE id IN secret_set AND 0 SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 0 SETTINGS $off"
check_access "UPDATE $CLICKHOUSE_DATABASE.tab SET name = (SELECT max(payload) FROM secret_tab) WHERE 0 SETTINGS $off, enable_lightweight_update = 1"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = dictGet('dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE $CLICKHOUSE_DATABASE.tab UPDATE name = joinGet('join_tab', 'payload', id) WHERE 0 SETTINGS $off"

echo "-- The value of an unreadable table never reached a readable column"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab WHERE name = 'TOP-SECRET'"

$CLICKHOUSE_CLIENT -q "
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS tab, xdb_tab, secret_tab, secret_set, join_tab, dict_src;
DROP USER IF EXISTS $user_name;
DROP DATABASE IF EXISTS $other_db;
"
