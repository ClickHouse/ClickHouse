#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user1="user02884_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user02884_2_${CLICKHOUSE_DATABASE}_$RANDOM"
user3="user02884_3_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table (s String) ENGINE = MergeTree ORDER BY s;

DROP USER IF EXISTS $user1, $user2, $user3;
CREATE USER $user1, $user2, $user3;
GRANT SELECT ON $db.* TO $user1;
EOF

echo "===== StorageView ====="
${CLICKHOUSE_CLIENT} <<EOF
CREATE VIEW $db.test_view_1 (s String)
AS SELECT * FROM $db.test_table;

CREATE DEFINER $user1 VIEW $db.test_view_2 (s String)
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_3 (s String)
DEFINER = $user1 SQL SECURITY DEFINER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_4 (s String)
DEFINER = $user1 SQL SECURITY INVOKER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_5 (s String)
SQL SECURITY INVOKER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_6 (s String)
SQL SECURITY DEFINER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_7 (s String)
DEFINER CURRENT_USER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_8 (s String)
DEFINER $user3
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_9 (s String)
SQL SECURITY NONE
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_10 (s String)
SQL SECURITY DEFINER
AS SELECT * FROM $db.test_table;

CREATE VIEW $db.test_view_11 (s String)
SQL SECURITY DEFINER
AS SELECT * FROM $db.test_table
WHERE s = {param_id:String};
EOF

(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE $db.test_view_5" 2>&1 | grep -c "INVOKER") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE $db.test_view_2" 2>&1 | grep -c "DEFINER = $user1") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} <<EOF
GRANT SELECT ON $db.test_view_1 TO $user2;
GRANT SELECT ON $db.test_view_2 TO $user2;
GRANT SELECT ON $db.test_view_3 TO $user2;
GRANT SELECT ON $db.test_view_4 TO $user2;
GRANT SELECT ON $db.test_view_5 TO $user2;
GRANT SELECT ON $db.test_view_6 TO $user2;
GRANT SELECT ON $db.test_view_7 TO $user2;
GRANT SELECT ON $db.test_view_8 TO $user2;
GRANT SELECT ON $db.test_view_9 TO $user2;
GRANT SELECT ON $db.test_view_10 TO $user2;
GRANT SELECT ON $db.test_view_11 TO $user2;
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
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_10"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_view_11(param_id='foo')"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.test_view_10 MODIFY SQL SECURITY INVOKER"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_10" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE $db.test_view_10" | grep -c "SQL SECURITY INVOKER"


echo "===== MaterializedView ====="
${CLICKHOUSE_CLIENT} --query "
  CREATE MATERIALIZED VIEW $db.test_mv_1 (s String)
  ENGINE = MergeTree ORDER BY s
  DEFINER = $user1 SQL SECURITY DEFINER
  AS SELECT * FROM $db.test_table;
"

(( $(${CLICKHOUSE_CLIENT} --query "
  CREATE MATERIALIZED VIEW $db.test_mv_2 (s String)
  ENGINE = MergeTree ORDER BY s
  SQL SECURITY INVOKER
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -c "SQL SECURITY INVOKER can't be specified for MATERIALIZED VIEW") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "
  CREATE MATERIALIZED VIEW $db.test_mv_3 (s String)
  ENGINE = MergeTree ORDER BY s
  SQL SECURITY NONE
  AS SELECT * FROM $db.test_table;
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE $db.test_mv_data (s String) ENGINE = MergeTree ORDER BY s;"

${CLICKHOUSE_CLIENT} --query "
  CREATE MATERIALIZED VIEW $db.test_mv_4
  TO $db.test_mv_data
  DEFINER = $user1 SQL SECURITY DEFINER
  AS SELECT * FROM $db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE MATERIALIZED VIEW $db.test_mv_5 (s String)
  ENGINE = MergeTree ORDER BY s
  DEFINER = $user2 SQL SECURITY DEFINER
  AS SELECT * FROM $db.test_table;
"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_5 TO $user2"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.test_mv_5 MODIFY SQL SECURITY NONE"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_5"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE $db.test_mv_5" | grep -c "SQL SECURITY NONE"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_1 TO $user2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_3 TO $user2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_4 TO $user2"

${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_1"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_3"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.test_mv_data FROM $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --materialized_views_ignore_errors 1 --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Failed to push block to view") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.test_mv_data TO $user1"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_mv_data TO $user1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');"
${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT count() FROM $db.test_mv_4"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.test_table FROM $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.test_table VALUES ('foo'), ('bar');" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.source
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a;

CREATE TABLE $db.destination1
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a;

CREATE TABLE $db.destination2
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a;

CREATE MATERIALIZED VIEW $db.mv1 TO $db.destination1
AS SELECT *
FROM $db.source;

ALTER TABLE $db.mv1 MODIFY DEFINER=default SQL SECURITY DEFINER;

CREATE MATERIALIZED VIEW $db.mv2 TO $db.destination2
AS SELECT *
FROM $db.destination1;
EOF

(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "INSERT INTO source SELECT * FROM generateRandom() LIMIT 100" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.source TO $user2"
${CLICKHOUSE_CLIENT} --user $user2 --query "INSERT INTO source SELECT * FROM generateRandom() LIMIT 100"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM destination1"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM destination2"

(( $(${CLICKHOUSE_CLIENT} --query "ALTER TABLE test_table MODIFY SQL SECURITY INVOKER" 2>&1 | grep -c "is not supported") >= 1 )) && echo "OK" || echo "UNEXPECTED"

(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "
  CREATE VIEW $db.test_view_broken
  SQL SECURITY DEFINER
  DEFINER CURRENT_USER
  DEFINER $user2
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -c "Syntax error") >= 1 )) && echo "Syntax error" || echo "UNEXPECTED"

echo "===== TestGrants ====="
${CLICKHOUSE_CLIENT} --query "GRANT CREATE ON *.* TO $user1"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_table TO $user1, $user2"

${CLICKHOUSE_CLIENT} --user $user1 --query "
  CREATE VIEW $db.test_view_g_1
  DEFINER = CURRENT_USER SQL SECURITY DEFINER
  AS SELECT * FROM $db.test_table;
"

(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "
  CREATE VIEW $db.test_view_g_2
  DEFINER = $user2
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $user2 TO $user1"

${CLICKHOUSE_CLIENT} --user $user1 --query "
  CREATE VIEW $db.test_view_g_2
  DEFINER = $user2
  AS SELECT * FROM $db.test_table;
"

(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "
  CREATE VIEW $db.test_view_g_3
  SQL SECURITY NONE
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $user2 TO $user1"

echo "===== TestRowPolicy ====="
${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_row_t (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;

CREATE VIEW $db.test_view_row_1 DEFINER = $user1 SQL SECURITY DEFINER AS SELECT x, y AS z FROM $db.test_row_t;
CREATE ROW POLICY r1 ON $db.test_row_t FOR SELECT USING x <= y TO $user1;
CREATE ROW POLICY r2 ON $db.test_view_row_1 FOR SELECT USING x >= z TO $user2;

INSERT INTO $db.test_row_t VALUES (1, 2), (1, 1), (2, 2), (3, 2), (4, 0);

GRANT SELECT ON $db.test_view_row_1 to $user2;
EOF

${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_view_row_1"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_row_t2 (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;

CREATE VIEW $db.test_mv_row_2 DEFINER = $user1 SQL SECURITY DEFINER AS SELECT x, y AS z FROM $db.test_row_t2;
CREATE ROW POLICY r1 ON $db.test_row_t2 FOR SELECT USING x <= y TO $user1;
CREATE ROW POLICY r2 ON $db.test_mv_row_2 FOR SELECT USING x >= z TO $user2;

INSERT INTO $db.test_row_t2 VALUES (5, 6), (6, 5), (6, 6), (8, 7), (9, 9);

GRANT SELECT ON $db.test_mv_row_2 to $user2;
EOF

${CLICKHOUSE_CLIENT} --user $user2 --query "SELECT * FROM $db.test_mv_row_2"

echo "===== TestInsertChain ====="

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.session_events(
    clientId UUID,
    sessionId UUID,
    pageId UUID,
    timestamp DateTime,
    type String
)
ENGINE = MergeTree
ORDER BY (timestamp);

CREATE TABLE $db.materialized_events(
    clientId UUID,
    sessionId UUID,
    pageId UUID,
    timestamp DateTime,
    type String
)
ENGINE = MergeTree
ORDER BY (timestamp);

CREATE MATERIALIZED VIEW $db.events_mv TO $db.materialized_events AS
SELECT
    clientId,
    sessionId,
    pageId,
    timestamp,
    type
FROM
    $db.session_events;

GRANT INSERT ON $db.session_events TO $user3;
GRANT SELECT ON $db.session_events TO $user3;
EOF

${CLICKHOUSE_CLIENT} --user $user3 --query "INSERT INTO $db.session_events SELECT * FROM generateRandom('clientId UUID, sessionId UUID, pageId UUID, timestamp DateTime, type Enum(\'type1\', \'type2\')', 1, 10, 2) LIMIT 1000"
${CLICKHOUSE_CLIENT} --user $user3 --query "SELECT count(*) FROM session_events"
${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM materialized_events"

echo "===== TestOnCluster ====="
${CLICKHOUSE_CLIENT} <<EOF

CREATE TABLE $db.test_cluster ON CLUSTER test_shard_localhost (a String) Engine = MergeTree() ORDER BY a FORMAT Null;
CREATE TABLE $db.test_cluster_2 ON CLUSTER test_shard_localhost (a String) Engine = MergeTree() ORDER BY a FORMAT Null;
CREATE MATERIALIZED VIEW $db.cluster_mv ON CLUSTER test_shard_localhost TO $db.test_cluster_2 AS SELECT * FROM $db.test_cluster FORMAT Null;
ALTER TABLE $db.cluster_mv ON CLUSTER test_shard_localhost MODIFY DEFINER = $user3 FORMAT Null;
EOF

${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE $db.cluster_mv" | grep -c "DEFINER = $user3"


${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2, $user3";
