#!/usr/bin/env bash
# Tags: no-fasttest, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The temporary file that BACKUP of a Memory table streams through is sized by
# temporary_files_buffer_size. max_compress_block_size must not reach it: it is a column
# compression setting, it has no lower bound, and a small value makes every compressed frame
# its own write, so the compressed stream grows larger than the data it encodes.
#
# Oracle is the byte ratio, not wall clock: compression must never expand. The uncompressed
# figure is fixed by the data, so the assertion is independent of machine speed, sanitizer,
# disk backend and of temporary_files_buffer_size itself.

backup_name="File('${CLICKHOUSE_TEST_UNIQUE_NAME}.zip')"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test;
CREATE TABLE test (x String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO test SELECT 'Hello, world' FROM numbers(1000000);
"

$CLICKHOUSE_CLIENT -m -q "
SET max_compress_block_size = 21;
BACKUP TABLE test TO $backup_name;
" --format Null

$CLICKHOUSE_CLIENT -m -q "
SELECT ProfileEvents['ExternalProcessingCompressedBytesTotal']
           <= ProfileEvents['ExternalProcessingUncompressedBytesTotal'] AS compression_did_not_expand
FROM system.backups WHERE name LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}%' AND status = 'BACKUP_CREATED';
"

$CLICKHOUSE_CLIENT -m -q "
TRUNCATE TABLE test;
RESTORE TABLE test FROM $backup_name;
" --format Null

$CLICKHOUSE_CLIENT -m -q "
SELECT count(), min(x), max(x) FROM test;
DROP TABLE test;
"
