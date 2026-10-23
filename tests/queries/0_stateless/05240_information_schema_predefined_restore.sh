#!/usr/bin/env bash
set -e

# `BACKUP DATABASE` includes every `information_schema` view under both name casings
# in both databases. `RESTORE DATABASE` must treat each view as predefined even when
# its saved definition differs from the current version; otherwise a newer view
# definition would fail the whole `RESTORE` with `CANNOT_RESTORE_TABLE`.

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
BACKUP DATABASE information_schema, DATABASE INFORMATION_SCHEMA
TO File('${WORK_DIR}/backups/b1') FORMAT Null;
"

# Simulate changed view definitions without changing the sizes recorded in the backup.
metadata_files=("${WORK_DIR}"/backups/b1/metadata/{information_schema,INFORMATION_SCHEMA}/*.sql)
if [ "${#metadata_files[@]}" -ne 56 ]; then
    echo "Expected 56 view definitions, found ${#metadata_files[@]}" >&2
    exit 1
fi
for metadata_file in "${metadata_files[@]}"; do
    if ! grep -q 'SQL SECURITY INVOKER' "$metadata_file"; then
        echo "Missing SQL SECURITY INVOKER in $metadata_file" >&2
        exit 1
    fi
    sed -i 's/SQL SECURITY INVOKER/SQL SECURITY DEFINER/' "$metadata_file"
done

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data_restored" -q "
RESTORE DATABASE information_schema, DATABASE INFORMATION_SCHEMA
FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
SELECT 'OK';
"

rm -rf "${WORK_DIR}"
