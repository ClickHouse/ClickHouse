#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: we restore from a zip-archived backup here.
# Tag no-parallel: we drop and restore fixed users and roles.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In this test we restore from "/tests/queries/0_stateless/backups/old_backup_without_access_entities_dependents.zip"
backup_name="$($CURDIR/helpers/install_predefined_backup.sh old_backup_without_access_entities_dependents.zip)"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS user_03231;
DROP ROLE IF EXISTS role_a_03231, role_b_03231;
"

${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM Disk('backups', '${backup_name}') FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER user_03231"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR user_03231"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE ROLE role_a_03231"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR role_a_03231"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE ROLE role_b_03231"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR role_b_03231"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER user_03231;
DROP ROLE role_a_03231, role_b_03231;
"
