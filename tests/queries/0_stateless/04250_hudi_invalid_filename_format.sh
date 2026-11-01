#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for STID 3160-3c8c (server fuzzer finding on master).
# Before the fix, `HudiMetadata::getDataFilesImpl` raised `LOGICAL_ERROR`
# when a parquet file in the table directory did not match the Hudi
# `[FileId]_[FileWriteToken]_[Timestamp].[ext]` naming convention.
# `LOGICAL_ERROR` is meant for internal invariants and aborts the server
# in debug/sanitizer builds via `chassert` / `std::terminate`. Since the
# filename comes from user-controlled storage contents, the parser must
# raise a regular user-facing exception (`INCORRECT_DATA`) instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="04250_hudi_bad_filename/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Upload a parquet file with an Iceberg-style filename (the exact pattern
# the fuzzer produced: `00000-0-<uuid>-0-00001.parquet`). Hudi expects
# the stem to split into exactly 3 parts on `_`; this one splits into 1.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/00000-0-7e41b120-8dbb-4f03-a487-fab888fe1037-0-00001.parquet')
    SETTINGS s3_truncate_on_insert = 1
    SELECT 1 AS a, 'hello' AS b
"

# Reading the directory as Hudi must raise INCORRECT_DATA, not LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM hudi(s3_conn, filename='${TABLE_PATH}/');
" 2>&1 | grep -o -E "INCORRECT_DATA|LOGICAL_ERROR" | head -1
