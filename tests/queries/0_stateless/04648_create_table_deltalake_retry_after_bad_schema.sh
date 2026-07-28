#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A CREATE TABLE whose schema cannot be represented in Delta must fail *before* commit 0 is written,
# leaving the target location clean, so that retrying at the same location with a valid schema succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_retry_after_bad_schema"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"

# (1) A CREATE with a type that has no compatible Delta type (`UInt64`) is rejected before commit 0 is
# written (Code: 48 = NOT_IMPLEMENTED), so the target location must be left clean (no orphan `_delta_log`).
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_retry (c UInt64) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
" 2>&1 | grep -q "Code: 48"; then
    echo "bad-schema: rejected"
else
    echo "bad-schema: NOT rejected"
fi
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_retry" >/dev/null 2>&1

if [ -d "${TABLE_PATH}/_delta_log" ]; then
    echo "bad-schema: fail: orphan _delta_log left behind"
else
    echo "bad-schema: no orphan _delta_log"
fi

# (2) Retrying at the SAME location with a valid schema must succeed and write commit 0.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_retry (c Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

if [ -f "$INITIAL_LOG" ]; then
    echo "retry: initial commit exists"
else
    echo "retry: fail: initial commit was not written"
fi

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SELECT count() FROM t_dl_retry;
DROP TABLE t_dl_retry;
"

rm -rf "$TABLE_PATH"
