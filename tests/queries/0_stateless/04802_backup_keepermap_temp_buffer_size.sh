#!/usr/bin/env bash
# Tags: no-ordinary-database, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Same property as 04801, for the KeeperMap backup path: the temporary file it streams rows
# through is sized by temporary_files_buffer_size, never by max_compress_block_size. That path
# is separate from the Memory one - a post-collecting task writing binary strings from Keeper
# instead of a native block stream - so it needs its own arm.
#
# Expansion is a per-frame property, so a small table shows it. Row count is what this test
# costs: it is one Keeper node per row, and dropping the table walks all of them, so keep it
# only as large as the ratio needs, which is any size at all.

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test SYNC;
CREATE TABLE test (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/test04802') PRIMARY KEY(key);
INSERT INTO test SELECT number, 'Hello, world' FROM numbers(200);
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
BACKUP TABLE test TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_tfbs.zip');
" --format Null
check_backup tfbs

$CLICKHOUSE_CLIENT -m -q "
TRUNCATE TABLE test;
RESTORE TABLE test FROM File('${CLICKHOUSE_TEST_UNIQUE_NAME}_mcbs.zip');
" --format Null

$CLICKHOUSE_CLIENT -m -q "
SELECT count(), min(value), max(value) FROM test;
DROP TABLE test SYNC;
"
