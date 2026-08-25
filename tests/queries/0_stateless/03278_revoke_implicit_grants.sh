#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03278_${CLICKHOUSE_DATABASE}_$RANDOM"
role1="role03278_1_${CLICKHOUSE_DATABASE}_$RANDOM"
role2="role03278_2_${CLICKHOUSE_DATABASE}_$RANDOM"


${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user;";

${CLICKHOUSE_CLIENT} <<EOF
CREATE USER $user;
CREATE ROLE $role1, $role2;

GRANT SELECT ON *.* TO $role1 WITH GRANT OPTION;
REVOKE SELECT ON test.table FROM $role1;

GRANT SELECT ON *.* TO $role2 WITH GRANT OPTION;
REVOKE SELECT ON test.table FROM $role2;
GRANT SHOW TABLES ON default.* TO $role2;

GRANT $role1 TO $user;
EOF

${CLICKHOUSE_CLIENT} --user $user --query "REVOKE ALL ON *.* FROM $role2"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $role2"
