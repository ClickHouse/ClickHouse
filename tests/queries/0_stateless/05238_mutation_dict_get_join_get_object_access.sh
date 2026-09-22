#!/usr/bin/env bash

# Tests that a mutation calling `dictGet` or `joinGet` requires the access on the object the call
# names - however the name is carried: a literal, a `WITH` alias, a query alias, an expression or a
# column - and that the requirement does not depend on `validate_mutation_query`. A background
# mutation runs with no user and therefore full access, so without the requirement an unprivileged
# user can read a dictionary or a `Join` table it has no grant on
# (https://github.com/ClickHouse/ClickHouse/issues/107588).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_05238"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, alias_tab, dim, join_tab, dict_src, d_src;
DROP DICTIONARY IF EXISTS dict;
DROP DICTIONARY IF EXISTS d;
DROP USER IF EXISTS $user_name;

-- The column 'dict' is named after the dictionary below on purpose: a carrier of that name must be
-- denied even under the grant on the dictionary, or the grant on it is a way to read any other one.
CREATE TABLE tab (id UInt32, name String, dict String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a', ''), (42, 'b', '');
CREATE TABLE dim (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim VALUES (1);

-- The dictionary and the Join table the user has no access to.
CREATE TABLE join_tab (id UInt32, payload String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO join_tab VALUES (1, 'joined');

CREATE TABLE dict_src (key UInt64, payload String) ENGINE = MergeTree ORDER BY key;
INSERT INTO dict_src VALUES (1, 'from-dict');
CREATE DICTIONARY dict (key UInt64, payload String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER UPDATE, UPDATE, SELECT(id, name) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.dim TO $user_name;
"

# Runs a query as the user.
function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
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

echo "-- dictGet and joinGet name their object instead of reading it as a column"
check_access "ALTER TABLE tab UPDATE name = dictGet('$CLICKHOUSE_DATABASE.dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"

# `dictGet` and `joinGet` take the name of their object from a `WITH` alias too - the analyzer
# resolves the identifier as an expression first - so such a name is not a reference to a CTE.
echo "-- The object of dictGet and joinGet named through a WITH alias is read as well"
check_access "ALTER TABLE tab UPDATE name = (WITH '$CLICKHOUSE_DATABASE.dict' AS d SELECT dictGet(d, 'payload', toUInt64(1))) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (WITH '$CLICKHOUSE_DATABASE.join_tab' AS j SELECT joinGet(j, 'payload', toUInt32(1))) WHERE 0 SETTINGS $off"
echo "-- A WITH alias that is not a string names an object that cannot be told, so the access is required on every object"
check_access "ALTER TABLE tab UPDATE name = (WITH materialize('$CLICKHOUSE_DATABASE.dict') AS d SELECT dictGet(d, 'payload', toUInt64(1))) WHERE 0 SETTINGS $off"

# The name of the object is taken from any constant `String` expression when the function is built,
# so an argument that names no one object here has to be treated as naming every one of them.
echo "-- An object named by an expression that is not one name is not one object either"
check_access "ALTER TABLE tab UPDATE name = dictGet(concat('$CLICKHOUSE_DATABASE', '.dict'), 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = joinGet(concat('$CLICKHOUSE_DATABASE', '.join_tab'), 'payload', id) WHERE 0 SETTINGS $off"

# An ordinary query alias carries the name as well as a `WITH` one does.
echo "-- An object named through a query alias is read as well"
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet(d, 'payload', toUInt64(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.dict' AS d) != '') WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT joinGet(j, 'payload', toUInt32(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.join_tab' AS j) != '') WHERE 0 SETTINGS $off"

# `resolveFunction.cpp` resolves the first argument through the expression scope of the query, not
# only through the aliases of the level it is written at, so a column a subquery below projects, and
# a column of the mutated table, carry the name just as well. Their value is not on the AST here, so
# such a carrier names every object and not an object of its own name. Each carrier below is named
# after an object the user is granted on further down, so taking it for that object - which is what
# reading the name off the AST does - would let it read any other object instead.
echo "-- An object named by a column a subquery below projects is not one name here"
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet(dict, 'payload', toUInt64(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.dict' AS dict) s) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet($CLICKHOUSE_DATABASE.dict, 'payload', toUInt64(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.dict' AS dict) AS $CLICKHOUSE_DATABASE) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT joinGet(join_tab, 'payload', toUInt32(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.join_tab' AS join_tab) s) WHERE 0 SETTINGS $off"
echo "-- A column of the mutated table does not name one object either"
check_access "ALTER TABLE tab UPDATE name = dictGet(dict, 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"

# `joinGet` probes the key columns of the `Join` table, not only the attribute it names, and
# `FunctionJoinGet::prepare` requires `SELECT` on both.
echo "-- joinGet reads the key columns of the Join table too"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(payload) ON $CLICKHOUSE_DATABASE.join_tab TO $user_name"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $CLICKHOUSE_DATABASE.join_tab TO $user_name"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"

$CLICKHOUSE_CLIENT -q "
GRANT SELECT ON $CLICKHOUSE_DATABASE.join_tab TO $user_name;
GRANT dictGet ON $CLICKHOUSE_DATABASE.dict TO $user_name;
"

echo "-- With the grants, the same mutations are allowed"
check_access "ALTER TABLE tab UPDATE name = dictGet('$CLICKHOUSE_DATABASE.dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"
# The grants are on the objects the aliases name, and never on an object of the alias' own name, so
# these pass only because the alias is followed to the object it stands for.
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet(d, 'payload', toUInt64(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.dict' AS d) != '') WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT joinGet(j, 'payload', toUInt32(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.join_tab' AS j) != '') WHERE 0 SETTINGS $off"

# A carrier that names no one object requires the access on every object, which the grants above -
# `dictGet` on the dictionary and `SELECT` on the `Join` table each carrier is named after - do not
# give. Reading the name off the AST instead would let every one of these read another object.
echo "-- A carrier that names no one object stays denied under the grants on the object it is named after"
check_access "ALTER TABLE tab UPDATE name = dictGet(concat('$CLICKHOUSE_DATABASE', '.dict'), 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet(dict, 'payload', toUInt64(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.dict' AS dict) s) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT dictGet($CLICKHOUSE_DATABASE.dict, 'payload', toUInt64(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.dict' AS dict) AS $CLICKHOUSE_DATABASE) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = (SELECT joinGet(join_tab, 'payload', toUInt32(1)) FROM (SELECT '$CLICKHOUSE_DATABASE.join_tab' AS join_tab) s) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = dictGet(dict, 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"

# The stored mutation must read the very object the check required. The mutation expression is
# qualified with the database of the mutated table before it is stored, and an alias of the query
# is not a dictionary name to qualify: `dictGet(d, ...)` with `'db.dict' AS d` in scope reads
# `db.dict`, and rewriting it to `dictGet(db.d, ...)` would make the stored mutation read a
# dictionary `d` of that database instead - here one the user has no grant on, with other values.
echo "-- The stored mutation reads the object the alias names, not a dictionary of the alias' own name"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE d_src (key UInt64, payload String) ENGINE = MergeTree ORDER BY key;
INSERT INTO d_src VALUES (1, 'from-d');
CREATE DICTIONARY d (key UInt64, payload String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'd_src' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE alias_tab (id UInt32, name String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO alias_tab VALUES (1, ''), (2, ''), (3, '');
GRANT ALTER UPDATE, UPDATE, SELECT ON $CLICKHOUSE_DATABASE.alias_tab TO $user_name;
"
check_access "ALTER TABLE alias_tab UPDATE name = (SELECT dictGet(d, 'payload', toUInt64(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.dict' AS d) != '') WHERE id = 1 SETTINGS $off"
check_access "ALTER TABLE alias_tab UPDATE name = (WITH '$CLICKHOUSE_DATABASE.dict' AS d SELECT dictGet(d, 'payload', toUInt64(1))) WHERE id = 2 SETTINGS $off"
check_access "UPDATE alias_tab SET name = (SELECT dictGet(d, 'payload', toUInt64(1)) FROM dim WHERE ('$CLICKHOUSE_DATABASE.dict' AS d) != '') WHERE id = 3 SETTINGS $off, enable_lightweight_update = 1"
$CLICKHOUSE_CLIENT -q "SELECT id, name FROM alias_tab ORDER BY id"
echo "-- A dictionary of the alias' own name is still one the user may not read by its name"
check_access "ALTER TABLE alias_tab UPDATE name = dictGet('$CLICKHOUSE_DATABASE.d', 'payload', toUInt64(1)) WHERE 0 SETTINGS $off"

$CLICKHOUSE_CLIENT -q "
DROP DICTIONARY IF EXISTS dict;
DROP DICTIONARY IF EXISTS d;
DROP TABLE IF EXISTS tab, alias_tab, dim, join_tab, dict_src, d_src;
DROP USER IF EXISTS $user_name;
"
