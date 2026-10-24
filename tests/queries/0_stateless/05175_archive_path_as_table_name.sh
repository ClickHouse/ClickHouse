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

echo '-- The interpretation of an ambiguous name is not shared between the two modes'
# A file whose name is literally the archive syntax, next to the archive of the same name: the
# `Filesystem` database caches a resolved table, and the two interpretations of the one name must
# not share that cache entry.
AMBIGUOUS="${CLICKHOUSE_TEST_UNIQUE_NAME}_ambiguous.tar"
echo -e "3,Literal" > "${USER_FILES_PATH}/${AMBIGUOUS}::${DATA}"
tar -C "${CLICKHOUSE_TMP}" -cf "${USER_FILES_PATH}/${AMBIGUOUS}" "${DATA}"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_ambiguous ENGINE = Filesystem"
for setting in 1 0 1 0
do
    echo "allow_archive_path_syntax = ${setting}"
    ${CLICKHOUSE_CLIENT} --allow_archive_path_syntax "${setting}" --query \
        "SELECT * FROM ${CLICKHOUSE_DATABASE}_ambiguous.\`${AMBIGUOUS}::${DATA}\` ORDER BY 1"
done
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_ambiguous"

rm "${USER_FILES_PATH}/${AMBIGUOUS}" "${USER_FILES_PATH}/${AMBIGUOUS}::${DATA}"

echo '-- A missing archive and a missing file inside an archive'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}_nonexistent.tar::${DATA}'" 2>&1 | grep -c 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM '${ARCHIVE}.tar::nonexistent.csv'" 2>&1 | grep -c 'UNKNOWN_TABLE'

echo '-- A file that is not in the archive is a missing table, not a table that fails to resolve'
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_missing_url ENGINE = URL('file://');
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_missing_fs ENGINE = Filesystem;
"
echo -n 'URL, the file is in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_missing_url.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.tar::${DATA}\`"
echo -n 'URL, the file is not in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_missing_url.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.tar::nonexistent.csv\`"
echo -n 'Filesystem, the file is in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_missing_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::${DATA}\`"
echo -n 'Filesystem, the file is not in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_missing_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::nonexistent.csv\`"

${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing_url.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.tar::nonexistent.csv\`" 2>&1 \
    | grep -om1 'UNKNOWN_TABLE'
${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::nonexistent.csv\`" 2>&1 \
    | grep -om1 'UNKNOWN_TABLE'

echo '-- A glob over the files inside an archive matches a dynamic set, so it is not probed'
echo -n 'Filesystem, a glob over the files in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_missing_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::*.csv\`"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM ${CLICKHOUSE_DATABASE}_missing_fs.\`${CLICKHOUSE_TEST_UNIQUE_NAME}.zip::*.csv\` ORDER BY 1;

    DROP DATABASE ${CLICKHOUSE_DATABASE}_missing_url;
    DROP DATABASE ${CLICKHOUSE_DATABASE}_missing_fs;
"

echo '-- A cached table is dropped once its file is no longer in the rewritten archive'
# The `Filesystem` database caches a resolved table. The archive stays in place while its contents
# change, so the cache entry must not outlive the file it was resolved for.
REWRITTEN="${CLICKHOUSE_TEST_UNIQUE_NAME}_rewritten.tar"
OTHER="${CLICKHOUSE_TEST_UNIQUE_NAME}_other.csv"
echo -e "4,Other" > "${CLICKHOUSE_TMP}/${OTHER}"
tar -C "${CLICKHOUSE_TMP}" -cf "${USER_FILES_PATH}/${REWRITTEN}" "${DATA}"

${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_rewritten ENGINE = Filesystem;
    SELECT * FROM ${CLICKHOUSE_DATABASE}_rewritten.\`${REWRITTEN}::${DATA}\` ORDER BY 1;
"
tar -C "${CLICKHOUSE_TMP}" -cf "${USER_FILES_PATH}/${REWRITTEN}" "${OTHER}"
echo -n 'Filesystem, the file is no longer in the archive: '
${CLICKHOUSE_CLIENT} --query \
    "EXISTS TABLE ${CLICKHOUSE_DATABASE}_rewritten.\`${REWRITTEN}::${DATA}\`"
${CLICKHOUSE_CLIENT} --query \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}_rewritten.\`${REWRITTEN}::${DATA}\`" 2>&1 \
    | grep -om1 'UNKNOWN_TABLE'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_rewritten"

rm "${USER_FILES_PATH}/${REWRITTEN}" "${CLICKHOUSE_TMP}/${OTHER}"

rm "${CLICKHOUSE_TMP}/${DATA}" "${ARCHIVE}.tar" "${ARCHIVE}.tar.zst" "${ARCHIVE}.zip"
