#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user1="user03279_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user03247_2_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}


${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user1, $user2;

CREATE TABLE $db.test_table1 (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id PARTITION BY id;
CREATE TABLE $db.test_table2 (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id PARTITION BY id;

INSERT INTO $db.test_table1 VALUES (1, 1);
INSERT INTO $db.test_table1 VALUES (2, 2);

CREATE USER $user1, $user2;
GRANT ALTER MOVE PARTITION ON $db.test_table1 TO $user1;
GRANT SELECT, ALTER DELETE ON $db.test_table1 TO $user2;
GRANT INSERT ON *.* TO $user1, $user2;
EOF

${CLICKHOUSE_CLIENT} --user $user1 --query "ALTER TABLE $db.test_table1 MOVE PARTITION 1 TO TABLE $db.test_table2;"
${CLICKHOUSE_CLIENT} --user $user2 --query "ALTER TABLE $db.test_table1 MOVE PARTITION 2 TO TABLE $db.test_table2;"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2";
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.test_table1, $db.test_table2";
