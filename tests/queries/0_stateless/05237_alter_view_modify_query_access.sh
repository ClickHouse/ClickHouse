#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
author="author_${CLICKHOUSE_DATABASE}"
definer="definer_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author, $definer;
CREATE USER $author, $definer IDENTIFIED WITH no_password;

CREATE TABLE $db.secret (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'SECRET');
CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;

GRANT SELECT ON $db.src TO $definer;
GRANT SELECT, INSERT ON $db.src TO $author;
GRANT CREATE TABLE, CREATE VIEW ON $db.* TO $author;
GRANT TABLE ENGINE ON MergeTree TO $author;
"

# Prints the error code the statement failed with, or `accepted` when it succeeded. Reporting the code
# rather than matching one expected code keeps a broken setup from reading as a successful statement.
run() {
    local err
    err=$(${CLICKHOUSE_CLIENT} --user "$author" --query "$2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
    echo "$1 ${err:-accepted}"
}

# The author is the definer of this view, so the SQL-security gate below never fires here and the
# table grants in the new body are what is under test.
${CLICKHOUSE_CLIENT} --user "$author" --query "
CREATE MATERIALIZED VIEW $db.mv_own ENGINE = MergeTree ORDER BY id
AS SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.mv_own TO $author"

echo '-- the protected table is reachable from the new body, so every shape must be denied'
run 'top level       ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, secret FROM $db.secret"
run 'from subquery   ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.secret)"
run 'cte             ' "ALTER TABLE $db.mv_own MODIFY QUERY WITH c AS (SELECT * FROM $db.secret) SELECT id, secret FROM c"
run 'join right side ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT e.id AS id, j.secret AS secret FROM $db.src AS e CROSS JOIN (SELECT * FROM $db.secret) AS j"
run 'union leg       ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src UNION ALL SELECT id, secret FROM (SELECT * FROM $db.secret)"

echo '-- only tables the user can read, so this must stay accepted'
run 'own subquery    ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, ''::String AS secret FROM (SELECT * FROM $db.src)"

echo '-- a column grant must not be rejected just because the read sits inside a subquery'
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(id) ON $db.secret TO $author"
run 'granted column  ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, ''::String AS secret FROM (SELECT id FROM $db.secret)"
run 'ungranted column' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, secret FROM (SELECT id, secret FROM $db.secret)"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(id) ON $db.secret FROM $author"

echo '-- rewriting the body of a view owned by another definer needs SET DEFINER on that definer'
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.mv_foreign ENGINE = MergeTree ORDER BY id
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.mv_foreign TO $author;
"
run 'foreign definer ' "ALTER TABLE $db.mv_foreign MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $definer TO $author"
run 'after the grant ' "ALTER TABLE $db.mv_foreign MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"

echo '-- a SQL SECURITY NONE body runs unchecked, so rewriting it needs ALLOW SQL SECURITY NONE'
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.mv_none ENGINE = MergeTree ORDER BY id
SQL SECURITY NONE
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.mv_none TO $author;
"
run 'security none   ' "ALTER TABLE $db.mv_none MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "GRANT ALLOW SQL SECURITY NONE ON *.* TO $author"
run 'after the grant ' "ALTER TABLE $db.mv_none MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"

echo '-- a MODIFY SQL SECURITY in the same statement decides what the body runs as, so the old one'
echo '-- must not be what gets authorized'
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.mv_migrate ENGINE = MergeTree ORDER BY id
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY, ALTER VIEW MODIFY SQL SECURITY, SELECT ON $db.mv_migrate TO $author;
REVOKE SET DEFINER ON $definer FROM $author;
"
run 'take over      ' "ALTER TABLE $db.mv_migrate MODIFY SQL SECURITY DEFINER DEFINER = CURRENT_USER, MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
run 'hand to other  ' "ALTER TABLE $db.mv_migrate MODIFY SQL SECURITY DEFINER DEFINER = $definer, MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
run 'still no read  ' "ALTER TABLE $db.mv_migrate MODIFY SQL SECURITY DEFINER DEFINER = CURRENT_USER, MODIFY QUERY SELECT id, secret FROM $db.secret"

echo '-- ON CLUSTER must not become a way around the checks above'
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $author"
run 'cluster top level' "ALTER TABLE $db.mv_own ON CLUSTER test_shard_localhost MODIFY QUERY SELECT id, secret FROM $db.secret"
run 'cluster subquery ' "ALTER TABLE $db.mv_own ON CLUSTER test_shard_localhost MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.secret)"

echo '-- no data leaked: none of the denied bodies was stored, so the view still reads only src'
run 'back to src     ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --user "$author" --query "INSERT INTO $db.src VALUES (1)"
echo -n 'rows, leaked    '
${CLICKHOUSE_CLIENT} --user "$author" --query "SELECT count(), countIf(secret != '') FROM $db.mv_own"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE $db.mv_own SYNC;
DROP TABLE $db.mv_foreign SYNC;
DROP TABLE $db.mv_none SYNC;
DROP TABLE $db.mv_migrate SYNC;
DROP USER $author, $definer;
"
