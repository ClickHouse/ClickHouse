#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# Regression test for issue #103155: CREATE TABLE against a location with no
# preexisting `_delta_log` must invoke the delta-kernel-rs create-table
# transaction and write the initial `00000000000000000000.json` commit.
# A subsequent CREATE TABLE against the same path is a no-op (the existing
# log is preserved, matching the prior attach-against-existing behavior).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_initial_log"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"

# Sanity check: starting from a clean slate, no _delta_log exists.
[ -d "${TABLE_PATH}/_delta_log" ] && echo "fail: _delta_log unexpectedly present before CREATE TABLE" && exit 1
echo "pre-create: no _delta_log"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

DROP TABLE IF EXISTS t_dl_initial;
CREATE TABLE t_dl_initial (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

# The kernel create-table transaction must have written commit version 0.
if [ ! -f "$INITIAL_LOG" ]; then
    echo "fail: initial commit was not written at $INITIAL_LOG"
    exit 1
fi
echo "post-create: initial commit exists"

# Capture the file size so we can prove the no-op CREATE does not overwrite it.
SIZE_BEFORE=$(wc -c < "$INITIAL_LOG")
MTIME_BEFORE=$(stat -f "%m" "$INITIAL_LOG" 2>/dev/null || stat -c "%Y" "$INITIAL_LOG")

# Round-trip a write through the kernel to confirm the new table is fully usable.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

INSERT INTO t_dl_initial SELECT number, toString(number) FROM numbers(3);
SELECT id, name FROM t_dl_initial ORDER BY id;
SELECT count() FROM t_dl_initial;
"

# Second CREATE TABLE against the same location must be a no-op:
# the original commit-0 file is preserved, and IF NOT EXISTS does not error.
sleep 1   # so any rewrite of the commit file would show up as a newer mtime
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

DROP TABLE t_dl_initial;
CREATE TABLE IF NOT EXISTS t_dl_initial (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT count() FROM t_dl_initial;
DROP TABLE t_dl_initial;
"

SIZE_AFTER=$(wc -c < "$INITIAL_LOG")
MTIME_AFTER=$(stat -f "%m" "$INITIAL_LOG" 2>/dev/null || stat -c "%Y" "$INITIAL_LOG")

if [ "$SIZE_BEFORE" != "$SIZE_AFTER" ] || [ "$MTIME_BEFORE" != "$MTIME_AFTER" ]; then
    echo "fail: initial commit file was rewritten by the second CREATE TABLE"
    exit 1
fi
echo "second-create: initial commit untouched"

rm -rf "$TABLE_PATH"
