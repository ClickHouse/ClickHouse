#!/usr/bin/env bash
# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user03702_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
CREATE TABLE $db.test_table (x int) ORDER BY x;
GRANT CREATE TABLE ON *.* TO $user;
GRANT TABLE ENGINE ON * TO $user;
DROP TABLE IF EXISTS test_copy;
EOF

${CLICKHOUSE_CLIENT} --user $user --query "CREATE TABLE $db.test_copy AS $db.test_table; -- { serverError ACCESS_DENIED }";

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.test_table TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "CREATE TABLE $db.test_copy AS $db.test_table"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
EOF
