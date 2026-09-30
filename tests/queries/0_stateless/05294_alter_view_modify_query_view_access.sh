#!/usr/bin/env bash
# Tags: no-ordinary-database
# no-ordinary-database: refreshable materialized views require an `Atomic` database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
author="author_${CLICKHOUSE_DATABASE}"
definer="definer_${CLICKHOUSE_DATABASE}"
view_definer="view_definer_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author, $definer, $view_definer;
CREATE USER $author, $definer, $view_definer IDENTIFIED WITH no_password;

CREATE TABLE $db.secret (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'SECRET');
CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.tgt (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;

CREATE VIEW $db.plain_view AS SELECT id, secret FROM $db.secret;
CREATE VIEW $db.param_view AS SELECT id, secret FROM $db.secret WHERE id = {x:UInt64};
GRANT SELECT ON $db.secret TO $view_definer;
CREATE VIEW $db.definer_param_view DEFINER = $view_definer SQL SECURITY DEFINER
AS SELECT id, secret FROM $db.secret WHERE id = {x:UInt64};

GRANT SELECT ON $db.src TO $definer;
GRANT INSERT ON $db.tgt TO $definer;

-- The author may read src and tgt and may act as the definer, but holds nothing on secret or on
-- either view, so only the new body decides the checks below.
GRANT SELECT, INSERT ON $db.src TO $author;
GRANT SELECT, INSERT ON $db.tgt TO $author;
GRANT CREATE TABLE, CREATE VIEW ON $db.* TO $author;
GRANT TABLE ENGINE ON MergeTree TO $author;
GRANT SET DEFINER ON $definer TO $author;
GRANT CLUSTER ON *.* TO $author;

CREATE DATABASE ${db}_session;
GRANT SHOW DATABASES ON ${db}_session.* TO $author;
"

${CLICKHOUSE_CLIENT} --user "$author" --query "
CREATE MATERIALIZED VIEW $db.mv ENGINE = MergeTree ORDER BY id
AS SELECT id, ''::String AS secret FROM $db.src"
# APPEND: a Replicated database refuses a replacing refresh into a non-replicated target.
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.rmv REFRESH EVERY 1 YEAR APPEND TO $db.tgt
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "
GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.mv TO $author;
GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.rmv TO $author;
"

# Prints the error code the statement failed with, or `accepted` when it succeeded. Reporting the code
# rather than matching one expected code keeps a broken setup from reading as a successful statement.
run() {
    local err
    err=$(${CLICKHOUSE_CLIENT} --user "$author" --query "$2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
    echo "$1 ${err:-accepted}"
}

echo '-- a view puts the protected table one level further away, which must not lose the check'
run 'plain view          ' "ALTER TABLE $db.mv MODIFY QUERY SELECT id, secret FROM $db.plain_view"
run 'plain view subquery ' "ALTER TABLE $db.mv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.plain_view)"

echo '-- a materialized view cannot read a table function at all, at any depth'
run 'param view          ' "ALTER TABLE $db.mv MODIFY QUERY SELECT id, secret FROM $db.param_view(x = 1)"
run 'param view subquery ' "ALTER TABLE $db.mv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.param_view(x = 1))"

echo '-- a refreshable view does accept a table function, so the access check has to carry it'
run 'refresh param       ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM $db.param_view(x = 1)"
run 'refresh param subq  ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.param_view(x = 1))"
run 'refresh plain subq  ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.plain_view)"

echo '-- a parameterized view that runs as its definer is checked on the view itself, at any depth'
run 'definer param       ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM $db.definer_param_view(x = 1)"
run 'definer param subq  ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.definer_param_view(x = 1))"
run 'create, param subq  ' "CREATE MATERIALIZED VIEW $db.rmv_new REFRESH EVERY 1 YEAR APPEND TO $db.tgt DEFINER = $definer SQL SECURITY DEFINER AS SELECT id, secret FROM (SELECT * FROM $db.definer_param_view(x = 1))"

echo '-- an unqualified name resolves in the database of the view, not of the session'
run 'session database    ' "USE ${db}_session; ALTER TABLE $db.rmv MODIFY QUERY SELECT id, secret FROM param_view(x = 1)"
run 'session db, cluster ' "USE ${db}_session; ALTER TABLE $db.rmv ON CLUSTER test_shard_localhost MODIFY QUERY SELECT id, secret FROM param_view(x = 1)"

echo '-- a table the author can read stays accepted, at both depths'
run 'own table           ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
run 'own table subquery  ' "ALTER TABLE $db.rmv MODIFY QUERY SELECT id, ''::String AS secret FROM (SELECT * FROM $db.src)"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE $db.rmv SYNC;
DROP TABLE $db.mv SYNC;
DROP TABLE IF EXISTS $db.rmv_new SYNC;
DROP VIEW $db.definer_param_view SYNC;
DROP DATABASE ${db}_session;
DROP USER $author, $definer, $view_definer;
"
