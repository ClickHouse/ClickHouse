#!/usr/bin/env bash
# A `Merge` table over `_temporary_and_external_tables` created before the database became
# forbidden must remain restorable from a backup: `RESTORE` (like replicated-database DDL
# replay) brings back previously stored metadata, not fresh user input, so only reading
# from the table is denied. The backup is prepared from a legitimate definition and then
# edited on disk, because such a table can no longer be created directly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=${CLICKHOUSE_TMP}/04813_merge_temporary_database_restore_${CLICKHOUSE_DATABASE}
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

# The source database name has the same length as `_temporary_and_external_tables`,
# so the substitution below preserves the file size recorded in the backup metadata.
SRC_DB=db_0123456789012345678901234567

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data" -q "
CREATE DATABASE ${SRC_DB};
CREATE TABLE ${SRC_DB}.src (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE m (x UInt8) ENGINE = Merge('${SRC_DB}', '^src\$');
BACKUP TABLE m TO File('${WORK_DIR}/backups/b1') FORMAT Null;
"

sed -i "s/${SRC_DB}/_temporary_and_external_tables/" "${WORK_DIR}/backups/b1/metadata/default/m.sql"

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data_restored" -q "
RESTORE TABLE default.m FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
SHOW CREATE TABLE m;
-- Introspection stays best-effort for the restored table: the size columns of \`system.columns\`
-- go through \`StorageMerge::tryGetColumnSizes\`, which must not throw for the forbidden database.
SELECT name, type, data_compressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 'm';
SELECT * FROM m; -- { serverError DATABASE_ACCESS_DENIED }
"

rm -rf "${WORK_DIR}"
