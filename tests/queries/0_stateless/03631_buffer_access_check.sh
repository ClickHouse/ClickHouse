#!/usr/bin/env bash
# Tags: long, no-replicated-database, no-async-insert

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user="user03631_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO $db.test_table VALUES ('foo');

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT, CREATE, INSERT ON $db.test_buffer TO $user;
GRANT TABLE ENGINE ON Buffer TO $user;
-- Creating the buffer without a column list infers the structure from the destination, which needs
-- SHOW COLUMNS on it. The read and write checks asserted below require SELECT and INSERT instead.
GRANT SHOW COLUMNS ON $db.test_table TO $user;
EOF

${CLICKHOUSE_CLIENT} --user $user --query "CREATE TABLE $db.test_buffer ENGINE = Buffer($db, test_table, 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.test_buffer" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user --query "INSERT INTO $db.test_buffer VALUES ('bar')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_table TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.test_buffer"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table_2 (s String, secret String) ENGINE = MergeTree ORDER BY s;
INSERT INTO $db.test_table_2 VALUES ('foo', 'classified');
CREATE TABLE $db.test_buffer_2 (s String, secret String, x String DEFAULT secret)
    ENGINE = Buffer($db, test_table_2, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
GRANT SELECT ON $db.test_buffer_2 TO $user;
GRANT SELECT(s, x) ON $db.test_table_2 TO $user;
EOF
# `x` is missing in the destination and its default reads `secret`, which the user may not read there.
(( $(${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT x FROM $db.test_buffer_2" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT s FROM $db.test_buffer_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret) ON $db.test_table_2 TO $user"
${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT x FROM $db.test_buffer_2"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table_3 (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO $db.test_table_3 VALUES ('foo');
CREATE TABLE $db.test_buffer_3 (s String, x UInt8 DEFAULT 0)
    ENGINE = Buffer($db, test_table_3, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
GRANT SELECT ON $db.test_buffer_3 TO $user;
GRANT SELECT(x) ON $db.test_table_3 TO $user;
EOF
# Only `x` is read and the destination lacks it, so the row count comes from a destination column the user may not read.
(( $(${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT x FROM $db.test_buffer_3" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(s) ON $db.test_table_3 TO $user"
${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT x FROM $db.test_buffer_3"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table_4 (allowed String, secret UInt8) ENGINE = MergeTree ORDER BY allowed;
INSERT INTO $db.test_table_4 VALUES ('foo', 1);
CREATE TABLE $db.test_alias_4 ENGINE = Alias(currentDatabase(), 'test_table_4');
CREATE TABLE $db.test_buffer_4 (allowed String, x UInt8 DEFAULT 0)
    ENGINE = Buffer($db, test_alias_4, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
GRANT SELECT ON $db.test_buffer_4 TO $user;
GRANT SELECT(allowed, x) ON $db.test_alias_4 TO $user;
GRANT SELECT(allowed) ON $db.test_table_4 TO $user;

CREATE TABLE $db.test_table_5 (k UInt32, n Nested(a UInt8, c UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.test_table_5 VALUES (1, [1, 2], [3, 4]);
CREATE TABLE $db.test_buffer_5 (k UInt32, n Nested(a UInt8, b UInt8, c UInt8))
    ENGINE = Buffer($db, test_table_5, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
GRANT SELECT ON $db.test_buffer_5 TO $user;
GRANT SELECT(\`n.b\`, \`n.c\`) ON $db.test_table_5 TO $user;
EOF
# The row count and the sizes of `n.b` are read from destination columns the user may read.
${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT x FROM $db.test_buffer_4"
${CLICKHOUSE_CLIENT} --user $user --send_logs_level=error --query "SELECT n.b FROM $db.test_buffer_5"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
