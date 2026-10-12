#!/usr/bin/env bash
# Tags: no-replicated-database
# Regression for the loop() metadata-disclosure: DESCRIBE loop(db, table) must
# require SHOW COLUMNS, exactly like a direct DESCRIBE of the table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04411_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secret (user_id UInt64, password_hash String) ENGINE = MergeTree ORDER BY user_id;
INSERT INTO $db.secret VALUES (1, 'hash');

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

# Zero grants on the table: schema must not leak through any DESCRIBE form.
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE $db.secret FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop('$db', 'secret') FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop($db.secret) FORMAT Null; -- { serverError ACCESS_DENIED }"

# SHOW COLUMNS is enough to read the schema (same as a direct DESCRIBE) but not the data.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.secret TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop('$db', 'secret') FORMAT TSV"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM loop('$db', 'secret') LIMIT 1; -- { serverError ACCESS_DENIED }"

# An inner table function with no source object (numbers) needs no extra grant.
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop(numbers(3)) FORMAT TSV"

# An inner table function with a source (file) must enforce its source access on
# the structure path too, so reverting it to raw getActualTableStructure regresses.
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop(file('nonexistent.csv', 'CSV', 'x UInt32, secret String')) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE loop(file('nonexistent.csv', 'CSV', 'x UInt32, secret String')) FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
