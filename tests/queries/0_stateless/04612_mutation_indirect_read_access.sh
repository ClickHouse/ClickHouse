#!/usr/bin/env bash

# Tests that a mutation requires access to what it reads indirectly - through a subquery, a table on
# the right of `IN`, `dictGet`, `joinGet` or a SQL UDF body - on every entry point, and that the
# requirement does not depend on `validate_mutation_query`. A background mutation runs with no user
# and therefore full access, and `validate_mutation_query = 0` skips the submission-time validation
# that would otherwise check these reads, so without the requirement an unprivileged user can read a
# table it has no `SELECT` on (https://github.com/ClickHouse/ClickHouse/issues/107588).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_04612"
udf_name="${CLICKHOUSE_DATABASE}_leak_04612"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, arr_tab, secret_tab, secret_set, join_tab, dict_src;
DROP DICTIONARY IF EXISTS dict;
DROP FUNCTION IF EXISTS $udf_name;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (id UInt32, name String, hidden UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a', 7), (42, 'b', 8);

-- A table of its own for the cases whose mutation cannot execute, so that a mutation left behind
-- does not merge into the predicates of the other cases.
CREATE TABLE arr_tab (id UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO arr_tab VALUES (1, [1]), (42, [42]);

-- The tables, set, dictionary and Join table the user has no access to.
CREATE TABLE secret_tab (secret UInt32, payload String) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (42, 'TOP-SECRET');

CREATE TABLE secret_set (secret UInt32) ENGINE = Set;
INSERT INTO secret_set VALUES (42);

CREATE TABLE join_tab (id UInt32, payload String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO join_tab VALUES (1, 'joined');

CREATE TABLE dict_src (key UInt64, payload String) ENGINE = MergeTree ORDER BY key;
INSERT INTO dict_src VALUES (1, 'from-dict');
CREATE DICTIONARY dict (key UInt64, payload String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);

-- A UDF body may reference a column that is not one of its parameters, so the call site alone does
-- not show which columns it reads.
CREATE FUNCTION $udf_name AS () -> hidden = 7;

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER UPDATE, ALTER DELETE, UPDATE, DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
GRANT ALTER DELETE ON $CLICKHOUSE_DATABASE.arr_tab TO $user_name;
-- The user can read and write 'id' and 'name', but not 'hidden', and has no grant at all on the
-- other objects.
GRANT SELECT(id, name) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
GRANT SELECT(id, arr) ON $CLICKHOUSE_DATABASE.arr_tab TO $user_name;
"

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

# Prints whether access control rejected the query, for the cases whose mutation cannot run to
# completion for a reason of its own, where only the access decision is the point.
function check_not_denied()
{
    if $CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1 | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "NOT_DENIED"
    fi
}

# Every case below is run with validation off, which is what makes the read invisible to the
# submission-time validation; the requirement has to come from the access check itself.
off="validate_mutation_query = 0, mutations_sync = 2"

echo "-- A subquery on an unreadable table, validation off"
check_access "ALTER TABLE tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"
check_access "DELETE FROM tab WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"
check_access "UPDATE tab SET name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS $off, enable_lightweight_update = 1"

echo "-- The subquery reads nothing of the target table, so its own columns are not the point"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT 1 FROM secret_tab WHERE payload = 'TOP-SECRET') SETTINGS $off"

echo "-- A table on the right of IN, validation off"
check_access "ALTER TABLE tab DELETE WHERE id IN secret_set SETTINGS $off"

# The right-hand side of IN is a table name, a set name or an array-valued column, and the three are
# the same identifier in the AST, so a column of the mutated table must not be mistaken for a table.
# Neither shape below can execute as a mutation, for a reason of its own that predates this check:
# the analyzer resolves the right-hand side of `IN` as a table name, and a `WITH` element of a
# mutation subquery is not in scope when the mutation runs. Both are exactly why the access check
# must not take such a name for a table, so the mutations are not waited for and only the access
# decision is asserted, on a table of their own that is dropped right after.
echo "-- An array column on the right of IN is a column, not a table"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE 1 IN arr AND 0 SETTINGS validate_mutation_query = 0"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE 1 IN arr_tab.arr AND 0 SETTINGS validate_mutation_query = 0"
echo "-- A WITH name on the right of IN is not a table either"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE id IN (WITH s AS (SELECT 1 AS v) SELECT v FROM s) AND 0 SETTINGS validate_mutation_query = 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE arr_tab SYNC"

echo "-- dictGet and joinGet name their object instead of reading it as a column"
check_access "ALTER TABLE tab UPDATE name = dictGet('$CLICKHOUSE_DATABASE.dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"

echo "-- A UDF body reading a column the user cannot read, on every entry point"
check_access "ALTER TABLE tab DELETE WHERE $udf_name() SETTINGS $off"
check_access "DELETE FROM tab WHERE $udf_name() SETTINGS $off"
check_access "UPDATE tab SET name = '' WHERE $udf_name() SETTINGS $off, enable_lightweight_update = 1"

echo "-- Unreadable columns are equally unreadable with validation on (the default)"
check_access "ALTER TABLE tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS mutations_sync = 2"
check_access "ALTER TABLE tab DELETE WHERE $udf_name() SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT -q "
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_set TO $user_name;
GRANT SELECT ON $CLICKHOUSE_DATABASE.join_tab TO $user_name;
GRANT dictGet ON $CLICKHOUSE_DATABASE.dict TO $user_name;
GRANT SELECT(hidden) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

echo "-- With the grants, the same mutations are allowed"
# The predicates keep every allowed mutation a no-op, so the secret is never actually copied.
check_access "ALTER TABLE tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "DELETE FROM tab WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN secret_set AND 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = dictGet('$CLICKHOUSE_DATABASE.dict', 'payload', toUInt64(id)) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab UPDATE name = joinGet('$CLICKHOUSE_DATABASE.join_tab', 'payload', id) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE $udf_name() AND 0 SETTINGS $off"
check_access "DELETE FROM tab WHERE $udf_name() AND 0 SETTINGS $off"
check_access "UPDATE tab SET name = name WHERE $udf_name() AND 0 SETTINGS $off, enable_lightweight_update = 1"

echo "-- The value of an unreadable table never reached a readable column"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab WHERE name = 'TOP-SECRET'"

$CLICKHOUSE_CLIENT -q "
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS tab, arr_tab, secret_tab, secret_set, join_tab, dict_src;
DROP FUNCTION IF EXISTS $udf_name;
DROP USER IF EXISTS $user_name;
"
