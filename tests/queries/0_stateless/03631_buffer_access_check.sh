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
EOF

${CLICKHOUSE_CLIENT} --user $user --query "CREATE TABLE $db.test_buffer ENGINE = Buffer($db, test_table, 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.test_buffer" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user --query "INSERT INTO $db.test_buffer VALUES ('bar')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.test_table TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM $db.test_buffer"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
