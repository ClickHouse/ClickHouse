#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03928_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secret (id UInt32, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'flag1'), (2, 'flag2');

DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH no_password;
GRANT SELECT ON $db.secret TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;

DROP ROW POLICY IF EXISTS rp_secret ON $db.secret;
CREATE ROW POLICY rp_secret ON $db.secret FOR SELECT USING id = 1 TO $user;
EOF

echo "--- Direct SELECT (row policy should filter to one row) ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.secret"

echo "--- loop() must also respect row policy ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM loop('$db', 'secret') LIMIT 4"

${CLICKHOUSE_CLIENT} <<EOF
DROP ROW POLICY IF EXISTS rp_secret ON $db.secret;
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.secret;
EOF
