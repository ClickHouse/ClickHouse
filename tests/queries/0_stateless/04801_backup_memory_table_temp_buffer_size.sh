#!/usr/bin/env bash
# Tags: no-fasttest, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The temporary file that BACKUP of a Memory table streams through is sized by
# temporary_files_buffer_size, never by max_compress_block_size: the latter is a column
# compression setting, it has no lower bound, and a small value makes every compressed frame
# its own write, so the compressed stream grows larger than the data it encodes.
#
# Oracle is the byte ratio, not wall clock: compression must never expand. The uncompressed
# figure is fixed by the data, so the assertion is independent of machine speed, sanitizer
# and disk backend. The accounted column fails if the byte accounting itself goes away, so
# the ratio cannot be satisfied by two absent counters.
#
# The two arms differ only in WHICH setting is made tiny, so together they say which setting
# reaches the buffer: only the temporary_files_buffer_size arm may expand.
#
# Expansion is a per-frame property, so the second arm shows it on a small table. It has its
# own, because temporary_files_buffer_size does reach the buffer here and a large fixture there
# would cost one write and one filesystem-cache reservation per 21 bytes.

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_small;
CREATE TABLE test (x String) ENGINE = Memory SETTINGS compress = 1;
CREATE TABLE test_small (x String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO test SELECT 'Hello, world' FROM numbers(1000000);
INSERT INTO test_small SELECT 'Hello, world' FROM numbers(10000);
"

function check_backup()
{
    $CLICKHOUSE_CLIENT -q "
    SELECT
        ProfileEvents['ExternalProcessingUncompressedBytesTotal'] > 0 AS accounted,
        ProfileEvents['ExternalProcessingCompressedBytesTotal']
            <= ProfileEvents['ExternalProcessingUncompressedBytesTotal'] AS compression_did_not_expand
    FROM system.backups
    WHERE name LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_$1%' AND status = 'BACKUP_CREATED';
    "
}

$CLICKHOUSE_CLIENT -m -q "
SET max_compress_block_size = 21;
BACKUP TABLE test TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_mcbs.zip');
" --format Null
check_backup mcbs

$CLICKHOUSE_CLIENT -m -q "
SET temporary_files_buffer_size = 21;
BACKUP TABLE test_small TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_tfbs.zip');
" --format Null
check_backup tfbs

$CLICKHOUSE_CLIENT -m -q "
TRUNCATE TABLE test;
RESTORE TABLE test FROM File('${CLICKHOUSE_TEST_UNIQUE_NAME}_mcbs.zip');
" --format Null

$CLICKHOUSE_CLIENT -m -q "
SELECT count(), min(x), max(x) FROM test;
DROP TABLE test;
DROP TABLE test_small;
"
