#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

user1="user03141_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user03141_2_${CLICKHOUSE_DATABASE}_$RANDOM"
user3="user03141_3_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE $db.test (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.test_table (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.test_table_prefix1 (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.test_table_prefix2 (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.test_table_prefix3 (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.test_table_another_prefix (s String) ENGINE = MergeTree ORDER BY s;

DROP USER IF EXISTS $user1, $user2, $user3;
CREATE USER $user1, $user2, $user3;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
GRANT SELECT ON $db.test_table* TO $user1;

GRANT SELECT ON $db.test_table_prefix* TO $user2;

GRANT SELECT ON $db.test_table* TO $user3;
REVOKE SELECT ON $db.test_table_prefix* FROM $user3;
EOF

(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM $db.test" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT count() FROM $db.test_table"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT count() FROM $db.test_table_prefix1"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT count() FROM $db.test_table_another_prefix"

(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_table" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_table_another_prefix" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_table_prefix1"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_table_prefix2"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_table_prefix3"

(( $(${CLICKHOUSE_CLIENT} --user $user3 --query "SELECT * FROM $db.test" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user3 --query "SELECT * FROM $db.test_table_prefix1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user3 --query "SELECT count() FROM $db.test_table"
${CLICKHOUSE_CLIENT} --user $user3 --query "SELECT count() FROM $db.test_table_another_prefix"

${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM $user1, $user2, $user3"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test TO $user1 WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} --user $user1 --query "GRANT SELECT ON $db.test TO $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "GRANT SELECT ON $db.test* TO $user1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "GRANT SELECT ON $db.test2* TO $user1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2, $user3"
