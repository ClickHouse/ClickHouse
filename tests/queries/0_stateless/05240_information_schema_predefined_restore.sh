#!/usr/bin/env bash
# All spellings of an `information_schema` view (lowercase and UPPERCASE, in both databases)
# are predefined tables, so RESTORE must skip them instead of comparing their definition
# with the one recorded in the backup: server-owned view definitions change between versions,
# and a mismatch would otherwise fail the whole RESTORE with CANNOT_RESTORE_TABLE.
# The backup metadata is edited on disk to simulate a backup taken on a different version.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=${CLICKHOUSE_TMP}/05240_information_schema_predefined_restore_${CLICKHOUSE_DATABASE}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data" -q "
BACKUP TABLE information_schema.user_privileges, TABLE information_schema.USER_PRIVILEGES,
       TABLE INFORMATION_SCHEMA.user_privileges, TABLE INFORMATION_SCHEMA.USER_PRIVILEGES
TO File('${WORK_DIR}/backups/b1') FORMAT Null;
"

# Simulate a backup taken on a version with a different view definition
# (the replacement preserves the file size recorded in the backup metadata).
sed -i "s/'def'/'dex'/" "${WORK_DIR}"/backups/b1/metadata/information_schema/*.sql "${WORK_DIR}"/backups/b1/metadata/INFORMATION_SCHEMA/*.sql

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data_restored" -q "
RESTORE TABLE information_schema.user_privileges FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
RESTORE TABLE information_schema.USER_PRIVILEGES FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
RESTORE TABLE INFORMATION_SCHEMA.user_privileges FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
RESTORE TABLE INFORMATION_SCHEMA.USER_PRIVILEGES FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
SELECT 'OK';
"

rm -rf "${WORK_DIR}"
