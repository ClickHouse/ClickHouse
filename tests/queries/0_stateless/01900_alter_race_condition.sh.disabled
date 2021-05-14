#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()" | tr '-' '_')
USER_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()" | tr '-' '_')

echo -e "DROP TABLE IF EXISTS merge_tree_user_column_privileges_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "CREATE TABLE merge_tree_user_column_privileges_${UUID} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = MergeTree() PARTITION BY y ORDER BY d" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "CREATE USER OR REPLACE user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "GRANT ALTER COLUMN(t1,t3,t4) ON merge_tree_user_column_privileges_${UUID} TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t2 TO t2_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t2 TO t2_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t2 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t2 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t1 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "OPTIMIZE TABLE merge_tree_user_column_privileges_${UUID} FINAL" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('hello')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES (30.01)" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} ORDER BY t1 FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t1 TO t1_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t1_new FROM merge_tree_user_column_privileges_${UUID} ORDER BY t1_new FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t1_new TO t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t1 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "DESCRIBE TABLE merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN fake_column" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t3 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "OPTIMIZE TABLE merge_tree_user_column_privileges_${UUID} FINAL" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('hello')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES (30.01)" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} ORDER BY t3 FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t3 TO t3_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t3_new FROM merge_tree_user_column_privileges_${UUID} ORDER BY t3_new FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t3_new TO t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t3 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "DESCRIBE TABLE merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN fake_column" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "SELECT t4 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t4" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t4" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "SELECT t4 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t4" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t4 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo -e "DROP USER IF EXISTS user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo -e "DROP TABLE IF EXISTS merge_tree_user_column_privileges_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
