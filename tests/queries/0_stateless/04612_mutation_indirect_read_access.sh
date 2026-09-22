#!/usr/bin/env bash

# Tests that a mutation requires access to what it reads indirectly - through a subquery, a table on
# the right of `IN`, `dictGet` or a SQL UDF body - on every entry point, and that the requirement
# does not depend on `validate_mutation_query`. A background mutation runs with no user and therefore
# full access, and `validate_mutation_query = 0` skips the submission-time validation that would
# otherwise check these reads, so without the requirement an unprivileged user can read a table it
# has no `SELECT` on (https://github.com/ClickHouse/ClickHouse/issues/107588).
#
# The object named by `dictGet` / `joinGet`, the reads through a table function and the binding of an
# unqualified name to the mutated table's database are covered by 05238, 05239 and 05240.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_04612"
udf_name="${CLICKHOUSE_DATABASE}_leak_04612"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, arr_tab, readable, dim, secret_tab, secret_set, dict_src;
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

-- The tables the user may read, with a column it may not, for the subqueries.
CREATE TABLE readable (id UInt32, arr Array(UInt32), hidden_arr Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO readable VALUES (1, [1], [7]);
CREATE TABLE dim (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim VALUES (1);

-- The tables, set and dictionary the user has no access to.
CREATE TABLE secret_tab (secret UInt32, payload String) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (42, 'TOP-SECRET');

CREATE TABLE secret_set (secret UInt32) ENGINE = Set;
INSERT INTO secret_set VALUES (42);

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
GRANT SELECT(id, arr) ON $CLICKHOUSE_DATABASE.readable TO $user_name;
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
# the same identifier in the AST. A one-part name is always a table there: the mutation expression is
# qualified with a database before it is stored, so `1 IN arr` becomes `1 IN (db.arr)` and reads a
# table of that name on every entry point - the array column is only read by a qualified `1 IN t.arr`.
echo "-- A one-part name on the right of IN is a table even where the mutated table has such a column"
check_access "ALTER TABLE arr_tab DELETE WHERE 1 IN arr AND 0 SETTINGS validate_mutation_query = 0, mutations_sync = 2"
echo "-- A qualified name that is an array column of the mutated table is a column"
check_access "ALTER TABLE arr_tab DELETE WHERE 1 IN arr_tab.arr AND 0 SETTINGS validate_mutation_query = 0, mutations_sync = 2"
# A `WITH` element of a mutation subquery is not in scope when the mutation runs, which predates this
# check and is exactly why the access check must not take such a name for a table, so the mutation is
# not waited for and only the access decision is asserted.
echo "-- A WITH name on the right of IN is not a table either, in its SELECT and in the subqueries below it"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE id IN (WITH s AS (SELECT 1 AS v) SELECT v FROM s) AND 0 SETTINGS validate_mutation_query = 0"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE id IN (WITH s AS (SELECT 1 AS v) SELECT v FROM (SELECT v FROM s)) AND 0 SETTINGS validate_mutation_query = 0"
echo "-- Once that subquery ends its WITH names are out of scope, and a later IN of the same name reads a table"
check_not_denied "ALTER TABLE arr_tab DELETE WHERE 1 IN (WITH secret_set AS (SELECT 0) SELECT 0) OR id IN secret_set SETTINGS validate_mutation_query = 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE arr_tab SYNC"

# A mutation is not executed in the session that submits it: the stored expression is replayed by a
# background mutation, which has no temporary tables of that session and no current database. A name
# a session temporary table answers is therefore not a grant-free name here - it is only readable
# there as a permanent table of that name.
echo "-- A session temporary table does not make a name grant-free in a mutation"
# The CI config sets `table_engines_require_grant`, under which the temporary table's `Memory` engine
# needs a grant of its own, so it is granted too and the session's own statements below go through.
$CLICKHOUSE_CLIENT -q "GRANT CREATE TEMPORARY TABLE ON *.*, TABLE ENGINE ON Memory TO $user_name"
# The session may create that temporary table and read it, so the denial below is the mutation's.
check_access "CREATE TEMPORARY TABLE secret_set (secret UInt32); SELECT count() FROM secret_set"
check_access "CREATE TEMPORARY TABLE secret_set (secret UInt32); ALTER TABLE tab DELETE WHERE id IN secret_set SETTINGS $off"

# A read named inside a subquery, or inside a `JOIN ... ON` condition, is invisible to a walk that
# only looks at the subquery's `FROM` tables and at the clauses of its `SELECT`.
echo "-- A named read below the top level is a read too, in a subquery and in a JOIN condition"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT id FROM readable WHERE id IN secret_set) SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT r.id FROM readable r JOIN dim d ON r.id = d.id AND r.id IN secret_set) SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT r.id FROM readable r JOIN dim d ON r.id = d.id AND dictGet('$CLICKHOUSE_DATABASE.dict', 'payload', toUInt64(r.id)) = '') SETTINGS $off"

echo "-- A column read only in an ARRAY JOIN list is read as well"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT id FROM readable ARRAY JOIN hidden_arr AS elem) SETTINGS $off"

# Below the top level a qualified name is resolved against the columns of the subquery's own tables,
# just as the mutated table's columns resolve it at the top level.
echo "-- An array column of the subquery's own table on the right of IN is a column, not a table"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT r.id FROM readable r WHERE 1 IN r.arr) AND 0 SETTINGS $off"

# An expression alias of a `SELECT` level is not a table either: `AddDefaultDatabaseVisitor` keeps a
# name on the right of `IN` as written when that level defines it as an alias, so the mutation reads
# a column of its own query and requiring `SELECT` on a table of that name would deny a mutation the
# user's grants allow. An `ARRAY JOIN` alias is collected the same way.
echo "-- An expression alias on the right of IN is not a table, an ARRAY JOIN alias included"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT 3 AS col3 FROM dim WHERE 3 IN col3) AND 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT id FROM readable ARRAY JOIN arr AS elem WHERE 1 IN elem) AND 0 SETTINGS $off"
# The alias goes out of scope with its level, so the same name below it, or after it, is a table again.
echo "-- That alias is out of scope in a nested SELECT and after its own level"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT 3 AS secret_set FROM dim WHERE id IN (SELECT id FROM dim WHERE 1 IN secret_set)) SETTINGS $off"

# A virtual column of a subquery's table needs no grant of its own, exactly as in a plain `SELECT`
# from that table, so requiring one would deny a mutation the equivalent `SELECT` is allowed to run.
echo "-- A virtual column of a subquery's table needs no grant"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT id FROM readable WHERE _part != '') AND 0 SETTINGS $off"

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
GRANT dictGet ON $CLICKHOUSE_DATABASE.dict TO $user_name;
GRANT SELECT(hidden) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

echo "-- With the grants, the same mutations are allowed"
# The predicates keep every allowed mutation a no-op, so the secret is never actually copied.
check_access "ALTER TABLE tab UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "DELETE FROM tab WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN secret_set AND 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE id IN (SELECT id FROM readable WHERE id IN secret_set) AND 0 SETTINGS $off"
check_access "ALTER TABLE tab DELETE WHERE $udf_name() AND 0 SETTINGS $off"
check_access "DELETE FROM tab WHERE $udf_name() AND 0 SETTINGS $off"
check_access "UPDATE tab SET name = name WHERE $udf_name() AND 0 SETTINGS $off, enable_lightweight_update = 1"

echo "-- The value of an unreadable table never reached a readable column"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab WHERE name = 'TOP-SECRET'"

$CLICKHOUSE_CLIENT -q "
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS tab, arr_tab, readable, dim, secret_tab, secret_set, dict_src;
DROP FUNCTION IF EXISTS $udf_name;
DROP USER IF EXISTS $user_name;
"
