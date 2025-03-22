#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

config="${BASH_SOURCE[0]/.sh/.xml}"

# Create a table on S3 disk with Array column and wide parts
# Insert a row with empty Array-s so that arr.bin file is empty
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS test_empty_blobs;

    CREATE TABLE test_empty_blobs (key Int, arr Array(UInt32)) ENGINE=MergeTree() ORDER BY tuple()
        SETTINGS disk='s3_disk', min_bytes_for_wide_part=1, min_rows_for_wide_part=1, max_file_name_length=127, min_bytes_for_full_part_storage=1;

    INSERT INTO test_empty_blobs SELECT *, [] from numbers(1);

    SELECT * FROM test_empty_blobs;

    SELECT part_name FROM system.parts WHERE database = currentDatabase() AND table = 'test_empty_blobs' AND active;
"

# Find table UUID for non-Ordinary database or use 'database_name/test_empty_blobs'
UUID=`$CLICKHOUSE_CLIENT -q "
    SELECT if (uuid != '00000000-0000-0000-0000-000000000000', uuid::String, currentDatabase() || '/test_empty_blobs')
    FROM system.tables
    WHERE database = currentDatabase() AND table = 'test_empty_blobs'"`;

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS blob_storage_log, text_log;

    -- Check that there were no empty blobs written to S3
    SELECT 'Empty blobs: ', local_path FROM system.blob_storage_log
    WHERE disk_name = 's3_disk' AND local_path LIKE '%$UUID/%all_1_1_0/%' AND
        data_size = 0 AND event_type = 'Upload' AND event_date >= yesterday() AND event_time > now() - interval 10 minute;

    -- Check logs for skipping empty blob
    SELECT 'Skipped empty blobs after 1 insert:',  count() FROM system.text_log
    WHERE message LIKE 'Skipping writing empty blob for path %$UUID/tmp_insert_all_1_1_0/arr.bin%' AND
        event_date >= yesterday() AND event_time > now() - interval 10 minute;

    -- Check that there are several non-empty blobs in part dir
    SELECT if (count() > 4, 'Non-empty blobs present', 'Test error: no blobs found') FROM system.blob_storage_log
    WHERE disk_name = 's3_disk' AND local_path LIKE '%$UUID/%all_1_1_0/%' AND
        data_size > 0 AND event_type = 'Upload' AND event_date >= yesterday() AND event_time > now() - interval 10 minute;

    -- Insert another row with empty Arrays to create another part with empty file
    INSERT INTO test_empty_blobs SELECT *, [] from numbers(1, 1);

    SELECT * FROM test_empty_blobs ORDER BY key;

    SELECT part_name FROM system.parts WHERE database = currentDatabase() AND table = 'test_empty_blobs' AND active;

    -- Initiate merge to test that it with empty files
    OPTIMIZE TABLE test_empty_blobs FINAL;

    -- Read after merge
    SELECT * FROM test_empty_blobs ORDER BY key;

    SELECT part_name FROM system.parts WHERE database = currentDatabase() AND table = 'test_empty_blobs' AND active;
";

BACKUP_NAME="test_empty_blobs_backup_$UUID"

$CLICKHOUSE_CLIENT -m -q "
    -- Backup and restore the table
    BACKUP TABLE test_empty_blobs TO Disk('backups', '$BACKUP_NAME');

    DROP TABLE test_empty_blobs;

    RESTORE TABLE test_empty_blobs FROM Disk('backups', '$BACKUP_NAME');
" >/dev/null && echo 'Backup-restore succeeded';

$CLICKHOUSE_CLIENT -m -q "
    -- Read after restore
    SELECT * FROM test_empty_blobs ORDER BY key;

    -- Check logs for skipping empty blob
    SYSTEM FLUSH LOGS text_log;
    SELECT 'Skipped empty blobs after 2 inserts and merge:',  count() FROM system.text_log WHERE 
        message LIKE 'Skipping writing empty blob for path %$UUID/%/arr.bin%' AND
        event_date >= yesterday() AND event_time > now() - interval 10 minute;
";

