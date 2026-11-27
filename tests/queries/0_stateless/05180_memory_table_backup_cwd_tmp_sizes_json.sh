#!/usr/bin/env bash
# Tags: memory-engine
# A file in the working directory must not be read when a `Memory` table is backed up:
# the `sizes.json` the backup ships is built in memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Absolute, because the working directory changes below.
WORK_DIR=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
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

cd "${WORK_DIR}" || exit 1

backup_manifest()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/db_$1" -q "
        CREATE TABLE m (a UInt64) ENGINE = Memory;
        INSERT INTO m VALUES (1);
        BACKUP TABLE m TO File('${WORK_DIR}/backups/$1') FORMAT Null;
    "
    cat "${WORK_DIR}/backups/$1/data/default/m/sizes.json"
}

# Nothing has created `tmp_sizes_json` yet, so this is the manifest the other arms must match.
CLEAN=$(backup_manifest clean)

: > "${WORK_DIR}/tmp_sizes_json"
UNPARSABLE=$(backup_manifest unparsable)

echo '{"clickhouse":{"foreign%2Efile%2Ebin":{"size":"424242"}}}' > "${WORK_DIR}/tmp_sizes_json"
WELL_FORMED=$(backup_manifest well_formed)

# The whole manifest is compared rather than pinned, because the recorded sizes depend on the
# compression settings; the key set is printed so a failure names what changed.
${CLICKHOUSE_LOCAL} -q "SELECT arraySort(JSONExtractKeys('${CLEAN}', 'clickhouse'))"
[ "${UNPARSABLE}" = "${CLEAN}" ] && echo "unparsable: identical to clean" || echo "unparsable: DIFFERS (${UNPARSABLE})"
[ "${WELL_FORMED}" = "${CLEAN}" ] && echo "well_formed: identical to clean" || echo "well_formed: DIFFERS (${WELL_FORMED})"

cd "${CUR_DIR}" && rm -rf "${WORK_DIR}"
