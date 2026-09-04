#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs libarchive and the `zip` utility.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table name can be a path using the archive syntax (`archive.tar::data.csv`), exactly like the
# argument of the `file` table function.

DATA="${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"
ARCHIVE="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

echo -e "1,Hello\n2,World" > "${CLICKHOUSE_TMP}/${DATA}"

tar -C "${CLICKHOUSE_TMP}" -cf "${ARCHIVE}.tar" "${DATA}"
tar -C "${CLICKHOUSE_TMP}" -caf "${ARCHIVE}.tar.zst" "${DATA}"
zip -q -j "${ARCHIVE}.zip" "${CLICKHOUSE_TMP}/${DATA}"

echo '-- The table function and the table name agree'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${ARCHIVE}.tar :: ${DATA}') ORDER BY 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.tar :: ${DATA}' ORDER BY 1"

echo '-- Every archive format'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.tar::${DATA}' ORDER BY 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.tar.zst::${DATA}' ORDER BY 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.zip::${DATA}' ORDER BY 1"

echo '-- A glob over the archives'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.{tar,zip}::${DATA}' ORDER BY 1, 2"

echo '-- Explicitly through the URL and the Filesystem database engines'
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_url ENGINE = URL('file://');
    SELECT * FROM ${CLICKHOUSE_DATABASE}_url.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.tar::${DATA}\` ORDER BY 1;
    DROP DATABASE ${CLICKHOUSE_DATABASE}_url;

    CREATE DATABASE ${CLICKHOUSE_DATABASE}_fs ENGINE = Filesystem;
    SELECT * FROM ${CLICKHOUSE_DATABASE}_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::${DATA}\` ORDER BY 1;
    DROP DATABASE ${CLICKHOUSE_DATABASE}_fs;
"

echo '-- The archive syntax can be switched off'
${CLICKHOUSE_LOCAL} --allow_archive_path_syntax 0 --query "SELECT * FROM '${ARCHIVE}.tar::${DATA}'" 2>&1 | grep -c 'UNKNOWN_TABLE'

echo '-- A missing archive and a missing file inside an archive'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}_nonexistent.tar::${DATA}'" 2>&1 | grep -c 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.tar::nonexistent.csv'" 2>&1 | grep -c 'CANNOT_EXTRACT_TABLE_STRUCTURE'

rm "${CLICKHOUSE_TMP}/${DATA}" "${ARCHIVE}.tar" "${ARCHIVE}.tar.zst" "${ARCHIVE}.zip"
