#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for a directory-boundary bypass in the `file()` function.
# The path check used a plain string-prefix test, so a *sibling* directory whose
# name merely begins with `user_files` (e.g. `.../user_files_evil`) was treated as
# being inside `user_files_path` and its contents could be read. A real boundary
# check must reject it.

# A sibling of user_files_path whose name starts with the same prefix. The unique
# suffix keeps concurrent runs of this test from colliding on the same directory.
EVIL_SUFFIX="_evil_${CLICKHOUSE_TEST_UNIQUE_NAME}"
EVIL_DIR="${USER_FILES_PATH}${EVIL_SUFFIX}"

cleanup() {
    rm -rf "${EVIL_DIR}"
    rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
}
trap cleanup EXIT

mkdir -p "${EVIL_DIR}"
echo -n "LEAKED" > "${EVIL_DIR}/secret.txt"

# A legitimate file nested inside user_files_path, to prove the boundary check
# does not over-reject paths that are genuinely inside the directory.
mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
echo -n "ok" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/inside.txt"

# The sibling directory must be rejected: the content of secret.txt must not leak.
# We only distinguish the access-denied rejection ("is not inside") from a leak.
echo "--- sibling directory (absolute path) ---"
${CLICKHOUSE_CLIENT} --query "SELECT file('${EVIL_DIR}/secret.txt')" 2>&1 | grep -o -m1 "is not inside\|LEAKED" || echo "UNEXPECTED"

# Same escape expressed as a relative path from user_files_path.
echo "--- sibling directory (relative path) ---"
${CLICKHOUSE_CLIENT} --query "SELECT file('../user_files${EVIL_SUFFIX}/secret.txt')" 2>&1 | grep -o -m1 "is not inside\|LEAKED" || echo "UNEXPECTED"

# A genuinely nested path is still allowed.
echo "--- nested path inside user_files ---"
${CLICKHOUSE_CLIENT} --query "SELECT file('${CLICKHOUSE_TEST_UNIQUE_NAME}/inside.txt')"
