#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
rdb="rdb_${CLICKHOUSE_DATABASE}"
author="author_${CLICKHOUSE_DATABASE}"

# Every replica of a `Replicated` database, the initiator included, applies the entry without the user,
# so the new body has to be checked before the entry is enqueued.
${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author;
CREATE USER $author IDENTIFIED WITH no_password;

CREATE TABLE $db.secret (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'SECRET');
CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE DATABASE $rdb ENGINE = Replicated('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rdb', '1', '1');

GRANT SELECT, INSERT ON $db.src TO $author;
GRANT CREATE TABLE, CREATE VIEW ON $rdb.* TO $author;
GRANT TABLE ENGINE ON MergeTree TO $author;
"
${CLICKHOUSE_CLIENT} --user "$author" --distributed_ddl_output_mode=none --query "
CREATE MATERIALIZED VIEW $rdb.mv ENGINE = MergeTree ORDER BY id
AS SELECT id, ''::String AS secret FROM $db.src"
${CLICKHOUSE_CLIENT} --query "GRANT ALTER VIEW MODIFY QUERY, SELECT ON $rdb.mv TO $author"

run() {
    local err
    err=$(${CLICKHOUSE_CLIENT} --user "$author" --query "$2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
    echo "$1 ${err:-accepted}"
}

run 'top level' "ALTER TABLE $rdb.mv MODIFY QUERY SELECT id, secret FROM $db.secret"
run 'subquery ' "ALTER TABLE $rdb.mv MODIFY QUERY SELECT id, secret FROM (SELECT * FROM $db.secret)"
run 'own table' "ALTER TABLE $rdb.mv MODIFY QUERY SELECT id, ''::String AS secret FROM $db.src"

${CLICKHOUSE_CLIENT} --user "$author" --query "INSERT INTO $db.src VALUES (1)"
echo -n 'rows, leaked '
${CLICKHOUSE_CLIENT} --user "$author" --query "SELECT count(), countIf(secret != '') FROM $rdb.mv"

${CLICKHOUSE_CLIENT} --query "
DROP DATABASE $rdb SYNC;
DROP USER $author;
"
