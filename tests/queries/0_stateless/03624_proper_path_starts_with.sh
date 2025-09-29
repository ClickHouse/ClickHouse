#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BAD_PATH_PARENT=$(echo "${USER_FILES_PATH}" | sed 's:/*$::')/../BAD
# Remove the trailing slashes and add extra characters to the folder name
BAD_PATH_EXTRA=$(echo "${USER_FILES_PATH}" | sed 's:/*$::')_BAD

$CLICKHOUSE_CLIENT --query "CREATE TABLE t (a UInt64) ENGINE=File('CSV', '${BAD_PATH_PARENT}') -- { serverError DATABASE_ACCESS_DENIED }"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t (a UInt64) ENGINE=File('CSV', '${BAD_PATH_EXTRA}') -- { serverError DATABASE_ACCESS_DENIED }"