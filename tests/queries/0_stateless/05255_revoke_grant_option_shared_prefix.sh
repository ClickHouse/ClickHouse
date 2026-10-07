#!/usr/bin/env bash
# Tests that "REVOKE GRANT OPTION FOR" is listed by SHOW GRANTS and by system.grants, and that the
# user can no longer pass the grant option on, also when another revoked database or table name
# shares a prefix with it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

grantor="user05255_1_${CLICKHOUSE_DATABASE}_$RANDOM"
victim1="user05255_2_${CLICKHOUSE_DATABASE}_$RANDOM"
victim2="user05255_3_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $grantor, $victim1, $victim2"
${CLICKHOUSE_CLIENT} --query "CREATE USER $grantor, $victim1, $victim2"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO $grantor WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON db1.* FROM $grantor"
${CLICKHOUSE_CLIENT} --query "REVOKE GRANT OPTION FOR SELECT ON db2.* FROM $grantor"
${CLICKHOUSE_CLIENT} --query "REVOKE GRANT OPTION FOR SELECT ON zdb2.* FROM $grantor"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON d.t1 FROM $grantor"
${CLICKHOUSE_CLIENT} --query "REVOKE GRANT OPTION FOR SELECT ON d.t2 FROM $grantor"
${CLICKHOUSE_CLIENT} --query "REVOKE GRANT OPTION FOR SELECT ON d.zt2 FROM $grantor"

echo "--- SHOW GRANTS"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $grantor" | sed "s/$grantor/grantor/g"

echo "--- system.grants"
${CLICKHOUSE_CLIENT} --query "
    SELECT database, table, access_type, grant_option, is_partial_revoke
    FROM system.grants WHERE user_name = '$grantor' ORDER BY database, table, access_type"

echo "--- the grant option cannot be passed on for a database whose grant option was revoked"
${CLICKHOUSE_CLIENT} --user "$grantor" --query "GRANT CURRENT GRANTS(SELECT ON db2.*) TO $victim1 WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $victim1" | sed "s/$victim1/victim1/g"

echo "--- positive control: a database that was never revoked is still passed on with grant option"
${CLICKHOUSE_CLIENT} --user "$grantor" --query "GRANT CURRENT GRANTS(SELECT ON db3.*) TO $victim2 WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $victim2" | sed "s/$victim2/victim2/g"

${CLICKHOUSE_CLIENT} --query "DROP USER $grantor, $victim1, $victim2"
