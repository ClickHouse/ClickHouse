#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A path with an embedded NUL byte must be rejected by every filesystem containment check: the check sees
# the whole string, while every syscall the path is later passed to stops at the NUL. Each case below names
# an existing entry inside `user_files` followed by a NUL and a suffix, so that a check which ignored the
# NUL would let the truncated path through and the query would succeed.

DIR="${USER_FILES_PATH}/nul_${CLICKHOUSE_DATABASE}"
mkdir -p "${DIR}"
printf 'data' > "${DIR}/file.csv"

echo "-- the file function checks with fileOrSymlinkPathStartsWith only"
${CLICKHOUSE_CLIENT} --query "SELECT file('nul_${CLICKHOUSE_DATABASE}/file.csv')"
${CLICKHOUSE_CLIENT} --query "SELECT file('nul_${CLICKHOUSE_DATABASE}/file.csv\0suffix') -- { serverError DATABASE_ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --query "SELECT file('${DIR}/file.csv\0suffix') -- { serverError DATABASE_ACCESS_DENIED }"

echo "-- the Filesystem database engine checks with pathStartsWith only"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE nul_${CLICKHOUSE_DATABASE} ENGINE = Filesystem('${DIR}\0suffix') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE nul_${CLICKHOUSE_DATABASE} ENGINE = Filesystem('nul_${CLICKHOUSE_DATABASE}\0suffix') -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE nul_${CLICKHOUSE_DATABASE} ENGINE = Filesystem('${DIR}')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM nul_${CLICKHOUSE_DATABASE}.\`file.csv\`"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE nul_${CLICKHOUSE_DATABASE}"

rm -r "${DIR}"
