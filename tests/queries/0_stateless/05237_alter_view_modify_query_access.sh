#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
author="author_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author;
CREATE USER $author IDENTIFIED WITH no_password;

CREATE TABLE $db.secret (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'SECRET');
CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;

GRANT SELECT, INSERT ON $db.src TO $author;
GRANT CREATE TABLE, CREATE VIEW ON $db.* TO $author;
GRANT TABLE ENGINE ON MergeTree TO $author;
"

run() {
    echo -n "$1 "
    ${CLICKHOUSE_CLIENT} --user "$author" --query "$2" 2>&1 \
        | grep -o -m1 'ACCESS_DENIED' || echo 'accepted'
}

# The author is the definer of this view, so only the table grants in the new body are under test.
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

echo '-- no data leaked: none of the denied bodies was stored, so the view still reads only src'
run 'back to src     ' "ALTER TABLE $db.mv_own MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --user "$author" --query "INSERT INTO $db.src VALUES (1)"
echo -n 'rows, leaked    '
${CLICKHOUSE_CLIENT} --user "$author" --query "SELECT count(), countIf(secret != '') FROM $db.mv_own"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv_own SYNC; DROP USER $author"
