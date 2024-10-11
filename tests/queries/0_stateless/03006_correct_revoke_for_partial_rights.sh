#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user1="user1_03006_${db}_$RANDOM"
user2="user2_03006_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $db;
CREATE DATABASE $db;
CREATE USER $user1, $user2;

GRANT SELECT ON *.* TO $user2 WITH GRANT OPTION;
REVOKE SELECT ON system.* FROM $user2;
EOF

${CLICKHOUSE_CLIENT} --user $user2 --query "GRANT CURRENT GRANTS ON *.* TO $user1"
${CLICKHOUSE_CLIENT} --user $user2 --query "REVOKE ALL ON *.* FROM $user1"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user1"

${CLICKHOUSE_CLIENT} --user $user2 --query "GRANT CURRENT GRANTS ON *.* TO $user1"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON $db.* FROM $user1"
${CLICKHOUSE_CLIENT} --user $user2 --query "REVOKE ALL ON *.* FROM $user1"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user1"

${CLICKHOUSE_CLIENT} --user $user2 --query "GRANT CURRENT GRANTS ON *.* TO $user1"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON $db.* FROM $user2"
${CLICKHOUSE_CLIENT} --user $user2 --query "REVOKE ALL ON *.* FROM $user1" 2>&1 | grep -c "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS $db"
