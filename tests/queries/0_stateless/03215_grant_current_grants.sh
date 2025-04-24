#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user1="user03215_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user03215_2_${CLICKHOUSE_DATABASE}_$RANDOM"
user3="user03215_3_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}


${CLICKHOUSE_CLIENT} --query "CREATE USER $user1, $user2, $user3;";
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, CREATE TABLE, CREATE VIEW ON $db.* TO $user1 WITH GRANT OPTION;";

${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON $db.* TO $user2" --user $user1;
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON $db.* TO $user3" --user $user2;

${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user2" | sed 's/ TO.*//';
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user3" | sed 's/ TO.*//';

${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS(SELECT ON $db.*) TO $user3" --user $user1;
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user3" | sed 's/ TO.*//';

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2, $user3";
