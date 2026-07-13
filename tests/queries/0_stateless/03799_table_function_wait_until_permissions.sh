#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03799_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

echo "--- Create table and user ---"
${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secret (id UInt32, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.secret VALUES (1, 'flag1'), (2, 'flag2');

DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH no_password;
EOF

echo "--- Direct SELECT (not enough privileges) ---"
${CLICKHOUSE_CLIENT} --user $user --server_logs_file=/dev/null --query "SELECT * FROM $db.secret" \
  2>&1 | grep -c 'Code: 497. DB::Exception: .* DB::Exception: .* Not enough privileges'

echo "--- waitUntil() (also not enough privileges) ---"
${CLICKHOUSE_CLIENT} --user $user --server_logs_file=/dev/null --query "SELECT * FROM waitUntil((SELECT count() == 2 FROM $db.secret))" \
  2>&1 | grep -c 'Code: 497. DB::Exception: .* DB::Exception: .* Not enough privileges'

echo "--- Grant to user ---"
${CLICKHOUSE_CLIENT} <<EOF
GRANT SELECT ON $db.secret TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

echo "--- Direct SELECT (has privileges) ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.secret"

echo "--- waitUntil() (also has privileges) ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM waitUntil((SELECT count() == 2 FROM $db.secret))"

echo "--- Create row policy to user ---"
${CLICKHOUSE_CLIENT} <<EOF
DROP ROW POLICY IF EXISTS rp_secret ON $db.secret;
CREATE ROW POLICY rp_secret ON $db.secret FOR SELECT USING id = 1 TO $user;
EOF

echo "--- Direct SELECT (row policy should filter to one row) ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.secret"

echo "--- waitUntil() must also respect row policy ---"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM waitUntil((SELECT count() == 1 FROM $db.secret))"

${CLICKHOUSE_CLIENT} <<EOF
DROP ROW POLICY IF EXISTS rp_secret ON $db.secret;
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.secret;
EOF
