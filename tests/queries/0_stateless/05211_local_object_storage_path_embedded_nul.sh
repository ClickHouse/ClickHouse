#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: Requires DeltaLake
# Tag no-msan: DeltaKernel is not compiled with msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A path with an embedded NUL byte must not pass the `user_files` containment check: the check sees the
# whole string, while every syscall the path is later passed to stops at the NUL. The path below is
# normalized into `user_files` as a whole string, but addresses `<user_files>/../probe_$CLICKHOUSE_DATABASE`
# once truncated at the NUL.

TAIL="../$(basename "${USER_FILES_PATH}")/nul_${CLICKHOUSE_DATABASE}"

echo "-- a path with an embedded NUL is rejected"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM deltaLakeLocal('${USER_FILES_PATH}/../probe_${CLICKHOUSE_DATABASE}\0/${TAIL}') -- { serverError PATH_ACCESS_DENIED }
"

echo "-- the same for icebergLocal"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/../probe_${CLICKHOUSE_DATABASE}\0/${TAIL}') -- { serverError PATH_ACCESS_DENIED }
"

echo "-- a path inside user_files is still accepted (and fails for its own reason)"
mkdir -p "${USER_FILES_PATH}/nul_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM deltaLakeLocal('${USER_FILES_PATH}/nul_${CLICKHOUSE_DATABASE}') -- { serverError DELTA_KERNEL_ERROR }
"
rmdir "${USER_FILES_PATH}/nul_${CLICKHOUSE_DATABASE}"
