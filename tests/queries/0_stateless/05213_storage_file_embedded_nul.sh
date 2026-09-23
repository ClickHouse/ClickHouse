#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `file` table function and the `File` engine stat the path and expand its globs on the filesystem before
# the matched paths are checked for containment in `user_files`, and every syscall stops at the first NUL byte.
# A path with an embedded NUL must therefore be rejected up front, before anything touches the filesystem.
# Each case names an existing entry followed by a NUL and a suffix, so that the truncated path would succeed.

DIR="${USER_FILES_PATH}/nul_${CLICKHOUSE_DATABASE}"
mkdir -p "${DIR}"
printf 'data\n' > "${DIR}/file.csv"

echo "-- the file table function"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('nul_${CLICKHOUSE_DATABASE}/file.csv', CSV, 'x String')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('nul_${CLICKHOUSE_DATABASE}/file.csv\0suffix', CSV, 'x String') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${DIR}/file.csv\0suffix', CSV, 'x String') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${DIR}/*\0suffix', CSV, 'x String') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${DIR}/file.csv\0suffix.zip :: file.csv', CSV, 'x String') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/file.csv\0suffix', CSV, 'x String') VALUES ('more') -- { serverError BAD_ARGUMENTS }"

echo "-- the File engine"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE nul_${CLICKHOUSE_DATABASE} (x String) ENGINE = File(CSV, 'nul_${CLICKHOUSE_DATABASE}/file.csv\0suffix') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE nul_${CLICKHOUSE_DATABASE} (x String) ENGINE = File(CSV, '${DIR}/file.csv\0suffix') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE nul_${CLICKHOUSE_DATABASE} (x String) ENGINE = File(CSV, 'nul_${CLICKHOUSE_DATABASE}/file.csv')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM nul_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE nul_${CLICKHOUSE_DATABASE}"

cat "${DIR}/file.csv"
rm -r "${DIR}"
