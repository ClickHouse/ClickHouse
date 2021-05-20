#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()" | tr '-' '_')
USER_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()" | tr '-' '_')

echo "step 1"
echo -e "DROP TABLE IF EXISTS merge_tree_user_column_privileges_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 2"
echo -e "CREATE TABLE merge_tree_user_column_privileges_${UUID} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = MergeTree() PARTITION BY y ORDER BY d" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 3"
echo -e "CREATE USER OR REPLACE user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 4"
echo -e "GRANT ALTER COLUMN(t1,t3,t4) ON merge_tree_user_column_privileges_${UUID} TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 5"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 6"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 7"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 8"
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 9"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 10"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 11"
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 12"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t2 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 13"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t2 TO t2_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 14"
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 15"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t2 TO t2_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 16"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t2 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 17"
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 18"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t2 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 19"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 20"
echo -e "GRANT NONE TO user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 21"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t2" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 22"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 23"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 24"
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 25"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 26"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 27"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 28"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 29"
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 30"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 31"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 32"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 33"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t1 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 34"
echo -e "OPTIMIZE TABLE merge_tree_user_column_privileges_${UUID} FINAL" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 35"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES ('hello')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 36"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t1) VALUES (30.01)" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 37"
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 38"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 39"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 40"
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} ORDER BY t1 FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 41"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t1 TO t1_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 42"
echo -e "SELECT t1 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 43"
echo -e "SELECT t1_new FROM merge_tree_user_column_privileges_${UUID} ORDER BY t1_new FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 44"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t1_new TO t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 45"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 46"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 47"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t1 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 48"
echo -e "DESCRIBE TABLE merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 49"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 50"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN fake_column" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 51"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t1 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 52"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 53"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t1" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 54"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 55"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 56"
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 57"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 58"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 59"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 60"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 61"
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 62"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 63"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 64"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 65"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t3 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 66"
echo -e "OPTIMIZE TABLE merge_tree_user_column_privileges_${UUID} FINAL" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 67"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES ('hello')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 68"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t3) VALUES (30.01)" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 69"
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 70"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 71"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 72"
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} ORDER BY t3 FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 73"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t3 TO t3_new" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 74"
echo -e "SELECT t3 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 75"
echo -e "SELECT t3_new FROM merge_tree_user_column_privileges_${UUID} ORDER BY t3_new FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 76"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} RENAME COLUMN t3_new TO t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 77"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 78"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 79"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} COMMENT COLUMN t3 'This is a comment.'" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 80"
echo -e "DESCRIBE TABLE merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 81"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 82"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN fake_column" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 83"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t3 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 84"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 85"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t3" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 86"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 87"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 88"
echo -e "SELECT t4 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 89"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t4" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 90"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 91"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('ready to be cleared')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 92"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} CLEAR COLUMN t4" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 93"
echo -e "SELECT t4 FROM merge_tree_user_column_privileges_${UUID} FORMAT JSONEachRow" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 94"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} DROP COLUMN t4" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 95"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} ADD COLUMN t4 String" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 96"
echo -e "INSERT INTO merge_tree_user_column_privileges_${UUID} (t4) VALUES ('3.4')" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 97"
echo -e "ALTER TABLE merge_tree_user_column_privileges_${UUID} MODIFY COLUMN t4 Float64" | ${CLICKHOUSE_CLIENT} -n --user "user_user_column_privileges_${USER_UUID}" > /dev/null 2>&1
echo $?
echo "step 98"
echo -e "DROP USER IF EXISTS user_user_column_privileges_${USER_UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
echo "step 99"
echo -e "DROP TABLE IF EXISTS merge_tree_user_column_privileges_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null 2>&1
echo $?
