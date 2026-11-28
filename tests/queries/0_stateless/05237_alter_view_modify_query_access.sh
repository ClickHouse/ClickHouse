#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
author="author_${CLICKHOUSE_DATABASE}"
definer="definer_${CLICKHOUSE_DATABASE}"

# The definer may read the protected table; the author may not.
${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author, $definer;
CREATE USER $author, $definer IDENTIFIED WITH no_password;

CREATE TABLE $db.secret (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'SECRET');
CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;

GRANT SELECT ON $db.src TO $definer;
GRANT SELECT ON $db.secret TO $definer;
GRANT SELECT, INSERT ON $db.src TO $author;
GRANT CLUSTER ON *.* TO $author;
"

# Prints the error code the statement failed with, or `accepted` when it succeeded. Reporting the code
# rather than matching one expected code keeps a broken setup from reading as a successful statement.
run() {
    local err
    err=$(${CLICKHOUSE_CLIENT} --user "${3:-$author}" --query "$2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
    echo "$1 ${err:-accepted}"
}

echo '-- the body of a view that runs as another user can be rewritten only with SET DEFINER on that user'
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.mv_foreign ENGINE = MergeTree ORDER BY id
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY, SELECT ON $db.mv_foreign TO $author;
"
reads_secret="SELECT e.id AS id, j.secret AS secret FROM $db.src AS e CROSS JOIN (SELECT * FROM $db.secret) AS j"
run 'reads secret    ' "ALTER TABLE $db.mv_foreign MODIFY QUERY $reads_secret"
run 'on cluster      ' "ALTER TABLE $db.mv_foreign ON CLUSTER test_shard_localhost MODIFY QUERY $reads_secret"
run 'reads only src  ' "ALTER TABLE $db.mv_foreign MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"

# The author may read the table, but a row policy hides every row from it. The body would run as the
# definer, which has no such policy, so only the SQL security gate can stop it.
${CLICKHOUSE_CLIENT} --query "
GRANT SELECT ON $db.secret TO $author;
CREATE ROW POLICY policy_$db ON $db.secret USING 0 TO $author;
"
run 'row policy      ' "ALTER TABLE $db.mv_foreign MODIFY QUERY $reads_secret"
${CLICKHOUSE_CLIENT} --query "
DROP ROW POLICY policy_$db ON $db.secret;
REVOKE SELECT ON $db.secret FROM $author;
"

${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $definer TO $author"
run 'after the grant ' "ALTER TABLE $db.mv_foreign MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "REVOKE SET DEFINER ON $definer FROM $author"

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
${CLICKHOUSE_CLIENT} --query "REVOKE ALLOW SQL SECURITY NONE ON *.* FROM $author"

echo '-- a MODIFY SQL SECURITY in the same statement decides what the body runs as, so the old one'
echo '-- must not be what gets authorized'
${CLICKHOUSE_CLIENT} --query "
CREATE MATERIALIZED VIEW $db.mv_migrate ENGINE = MergeTree ORDER BY id
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY, ALTER VIEW MODIFY SQL SECURITY, SELECT ON $db.mv_migrate TO $author;
"
run 'take over       ' "ALTER TABLE $db.mv_migrate MODIFY SQL SECURITY DEFINER DEFINER = CURRENT_USER, MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
run 'hand to other   ' "ALTER TABLE $db.mv_migrate MODIFY SQL SECURITY DEFINER DEFINER = $definer, MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"

echo '-- a definer named <user>:definer is a real user unless <user> is ephemeral'
real="${definer}:definer"
${CLICKHOUSE_CLIENT} --query "
CREATE USER \`$real\` IDENTIFIED WITH no_password;
GRANT SELECT ON $db.src TO \`$real\`;
CREATE MATERIALIZED VIEW $db.mv_real ENGINE = MergeTree ORDER BY id
DEFINER = \`$real\` SQL SECURITY DEFINER
AS SELECT id, ''::String AS secret FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY ON $db.mv_real TO $author, \`$real\`;
GRANT SET DEFINER ON $definer TO $author;
"
run 'real, base grant' "ALTER TABLE $db.mv_real MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"
run 'real, own view  ' "ALTER TABLE $db.mv_real MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src" "$real"
${CLICKHOUSE_CLIENT} --query "REVOKE SET DEFINER ON $definer FROM $author"

# A view this host does not have must be refused here, before dispatch: the hosts that have it run the
# entry without the user. The host rejects it too, but its error names the host.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER VIEW MODIFY QUERY ON $db.mv_nowhere TO $author"
out=$(${CLICKHOUSE_CLIENT} --user "$author" --query "
ALTER TABLE $db.mv_nowhere ON CLUSTER test_shard_localhost MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src" 2>&1)
code=$(echo "$out" | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
if echo "$out" | grep -q 'There was an error on'; then where='elsewhere'; else where='on the initiator'; fi
echo "cluster no view  ${code:-accepted} $where"

echo '-- no data leaked: none of the denied bodies was stored'
${CLICKHOUSE_CLIENT} --user "$author" --query "INSERT INTO $db.src VALUES (1)"
echo -n 'rows, leaked     '
${CLICKHOUSE_CLIENT} --user "$author" --query "SELECT count(), countIf(secret != '') FROM $db.mv_foreign"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE $db.mv_foreign SYNC;
DROP TABLE $db.mv_none SYNC;
DROP TABLE $db.mv_migrate SYNC;
DROP TABLE $db.mv_real SYNC;
DROP USER $author, $definer;
"
# Dropping its last view also removes a definer named <user>:definer.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS \`$real\`"
