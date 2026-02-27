#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BAD_PATH_PARENT=$(echo "${USER_FILES_PATH}" | sed 's:/*$::')/../BAD
# Remove the trailing slashes and add extra characters to the folder name
BAD_PATH_EXTRA=$(echo "${USER_FILES_PATH}" | sed 's:/*$::')_BAD

$CLICKHOUSE_CLIENT --query "CREATE TABLE t (a UInt64) ENGINE=File('CSV', '${BAD_PATH_PARENT}') -- { serverError DATABASE_ACCESS_DENIED }"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t (a UInt64) ENGINE=File('CSV', '${BAD_PATH_EXTRA}') -- { serverError DATABASE_ACCESS_DENIED }"

# Check the behaviour with dots in the path
# We allow paths like ./file or .file or ..file or a/../s.csv
# But not paths that go to the parent directory like ../BAD or dir/../../BAD

GOOD_PATH_WITH_DOT=$(echo "${USER_FILES_PATH}/.file_that_does_not_exist_but_is_in_a_valid_path.csv")
GOOD_PATH_WITH_DOTS=$(echo "${USER_FILES_PATH}/..file_that_does_not_exist_but_is_in_a_valid_path.csv")
GOOD_PATH_WITH_DOTS_BUT_INSIDE=$(echo "${USER_FILES_PATH}/..a/../s.csv")
BAD_PATH_PARENT_AFTER_SOME_DOTS=$(echo "${USER_FILES_PATH}" | sed 's:/*$::')/.DIR/../../BAD

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tdot, tdotdot, tdotdotin"

$CLICKHOUSE_CLIENT --query "CREATE TABLE tdot (a UInt64) ENGINE=File('CSV', '${GOOD_PATH_WITH_DOT}')"
$CLICKHOUSE_CLIENT --query "CREATE TABLE tdotdot (a UInt64) ENGINE=File('CSV', '${GOOD_PATH_WITH_DOTS}')"
$CLICKHOUSE_CLIENT --query "CREATE TABLE tdotdotin (a UInt64) ENGINE=File('CSV', '${GOOD_PATH_WITH_DOTS_BUT_INSIDE}')"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t (a UInt64) ENGINE=File('CSV', '${BAD_PATH_PARENT_AFTER_SOME_DOTS}') -- { serverError DATABASE_ACCESS_DENIED }"