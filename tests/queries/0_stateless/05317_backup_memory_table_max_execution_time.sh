#!/usr/bin/env bash
# Tags: no-fasttest, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# BACKUP of a Memory table must stop serializing the table's blocks once the query is past
# max_execution_time (or killed), instead of writing all of them first.
#
# temporary_files_buffer_size = 21 makes the serialization take far longer than the 1 s limit, and
# the finished data.bin larger than the 780 MB of table data (each 21-byte frame gets a header), so
# a backup that stops during the serialization writes less than 780 MB to the temporary file.

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t (x String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t SETTINGS ast_fuzzer_runs = 0, max_block_size = 100, min_insert_block_size_rows = 100,
    min_insert_block_size_bytes = 0 SELECT repeat('Hello, world', 1000) FROM numbers(65000);
"

$CLICKHOUSE_CLIENT -m -q "
SET temporary_files_buffer_size = 21, max_execution_time = 1, timeout_overflow_mode = 'throw';
BACKUP TABLE t TO Null SETTINGS id = '${CLICKHOUSE_TEST_UNIQUE_NAME}';
" > /dev/null 2>&1

$CLICKHOUSE_CLIENT -q "
SELECT status, error LIKE '%TIMEOUT_EXCEEDED%', ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] < 780000000
FROM system.backups WHERE id = '${CLICKHOUSE_TEST_UNIQUE_NAME}';
DROP TABLE t;
"
