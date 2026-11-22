#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the `zip` utility.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# For `file('archive.zip :: member.csv')` the pushed-down `_file` value is the
# member name and `_path` is `<archive>::<member>`, and the filter must run
# before the archive is opened: an excluded missing archive is not an error.
# A `GLOBAL IN` set is only ready when the pipeline runs, so the iterator has
# to defer the pruning.

FILE_PREFIX="05043_${CLICKHOUSE_DATABASE}"
MEMBER="${FILE_PREFIX}_entry.csv"
ARCHIVE="${USER_FILES_PATH}/${FILE_PREFIX}_archive.zip"
MISSING_ARCHIVE="${USER_FILES_PATH}/${FILE_PREFIX}_missing.zip"

echo -e "1\n2\n3" > "${CLICKHOUSE_TMP}/${MEMBER}"
zip -j -q "${ARCHIVE}" "${CLICKHOUSE_TMP}/${MEMBER}"

echo "_file equality, match"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: ${MEMBER}', CSV, 'x UInt64') WHERE _file = '${MEMBER}' ORDER BY x"
echo "_file equality, excluded"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: ${MEMBER}', CSV, 'x UInt64') WHERE _file = 'no_such.csv'"
echo "_path equality, match"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: ${MEMBER}', CSV, 'x UInt64') WHERE _path = '${ARCHIVE}::${MEMBER}' ORDER BY x"
echo "_file GLOBAL IN, match"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: ${MEMBER}', CSV, 'x UInt64') WHERE _file GLOBAL IN (SELECT '${MEMBER}') ORDER BY x"
echo "_file GLOBAL IN, excluded"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: ${MEMBER}', CSV, 'x UInt64') WHERE _file GLOBAL IN (SELECT 'no such file')"
echo "missing archive, excluded by _file GLOBAL IN before opening"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${MISSING_ARCHIVE} :: entry.csv', CSV, 'x UInt64') WHERE _file GLOBAL IN (SELECT 'no such file')"
echo "missing archive, excluded by _path GLOBAL IN before opening"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${MISSING_ARCHIVE} :: entry.csv', CSV, 'x UInt64') WHERE _path GLOBAL IN (SELECT 'no such path')"
echo "missing archive without a filter still throws"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${MISSING_ARCHIVE} :: entry.csv', CSV, 'x UInt64')" 2>&1 | grep -q "doesn't exist" && echo "OK" || echo "FAIL"

rm "${ARCHIVE}" "${CLICKHOUSE_TMP}/${MEMBER}"
