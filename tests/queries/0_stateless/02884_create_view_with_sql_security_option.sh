#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user1="user02884_1_$RANDOM"
user2="user02884_2_$RANDOM"
user3="user02884_3_$RANDOM"
db="db02884_$RANDOM"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP DATABASE IF EXISTS $db;
CREATE DATABASE $db;
CREATE TABLE $db.test_table (s String) ENGINE = MergeTree ORDER BY s;

DROP USER IF EXISTS $user1, $user2, $user3;
CREATE USER $user1, $user2, $user3;
GRANT SELECT ON $db.* TO $user1;
EOF

echo "===== StorageView ====="
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE VIEW $db.test_view_1 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER $user1 VIEW $db.test_view_2 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER = $user1 SQL SECURITY DEFINER VIEW $db.test_view_3 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER = $user1 SQL SECURITY INVOKER VIEW $db.test_view_4 (s String)
AS SELECT * FROM $db.test_table;

CREATE SQL SECURITY INVOKER VIEW $db.test_view_5 (s String)
AS SELECT * FROM $db.test_table;

CREATE SQL SECURITY DEFINER VIEW $db.test_view_6 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER CURRENT_USER VIEW $db.test_view_7 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER $user3 VIEW $db.test_view_8 (s String)
AS SELECT * FROM $db.test_table;

CREATE SQL SECURITY NONE VIEW $db.test_view_9 (s String)
AS SELECT * FROM $db.test_table;
EOF

(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE $db.test_view_1" 2>&1 | grep -c "DEFINER") >= 1 )) && echo "UNEXPECTED" || echo "OK"
(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE $db.test_view_2" 2>&1 | grep -c "DEFINER = $user1") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
GRANT SELECT ON $db.test_view_1 TO $user2;
GRANT SELECT ON $db.test_view_2 TO $user2;
GRANT SELECT ON $db.test_view_3 TO $user2;
GRANT SELECT ON $db.test_view_4 TO $user2;
GRANT SELECT ON $db.test_view_5 TO $user2;
GRANT SELECT ON $db.test_view_6 TO $user2;
GRANT SELECT ON $db.test_view_7 TO $user2;
GRANT SELECT ON $db.test_view_8 TO $user2;
GRANT SELECT ON $db.test_view_9 TO $user2;
EOF

${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');"

(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_2"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_3"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_5" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_6"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_7"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_8" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_9"


echo "===== MaterializedView ====="
${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER = $user1 SQL SECURITY DEFINER
  MATERIALIZED VIEW $db.test_mv_1 (s String)
  ENGINE = MergeTree ORDER BY s
  AS SELECT * FROM $db.test_table;
"

(( $(${CLICKHOUSE_CLIENT} --query "
  CREATE SQL SECURITY INVOKER
  MATERIALIZED VIEW $db.test_mv_2 (s String)
  ENGINE = MergeTree ORDER BY s
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -c "SQL SECURITY INVOKER can't be specified for MATERIALIZED VIEW") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "
  CREATE SQL SECURITY NONE
  MATERIALIZED VIEW $db.test_mv_3 (s String)
  ENGINE = MergeTree ORDER BY s
  AS SELECT * FROM $db.test_table;
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE $db.test_mv_data (s String) ENGINE = MergeTree ORDER BY s;"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER = $user1 SQL SECURITY DEFINER
  MATERIALIZED VIEW $db.test_mv_4
  TO $db.test_mv_data
  AS SELECT * FROM $db.test_table;
"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_1 TO $user2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_3 TO $user2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_4 TO $user2"

${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_1"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_3"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.test_mv_data FROM $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --stop_insert_if_fail_to_update_dependent_view 0 --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Failed to push block to view") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.test_mv_data TO $user1"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_data TO $user1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_4"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.test_table FROM $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS $db;"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2, $user3";
