#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03247_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user;";
${CLICKHOUSE_CLIENT} --query "CREATE USER $user;";

echo "Empty grants";
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user WITH IMPLICIT;" | sed 's/ TO.*//';

echo "Revoke grants";
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO $user ;";
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON test_03247.table FROM $user;";
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user WITH IMPLICIT;" | sed 's/ TO.*//' | sed 's/ FROM.*//';

(( $(${CLICKHOUSE_CLIENT} --user $user --query "EXISTS test_03247.table;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED";
${CLICKHOUSE_CLIENT} --query "EXISTS test_03247.table2;" --user $user;

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user;";
