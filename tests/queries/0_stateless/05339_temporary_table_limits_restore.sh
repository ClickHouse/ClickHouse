#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RESTORE TEMPORARY TABLE` checks `max_temporary_table_memory_usage`, `max_temporary_table_size_bytes_compressed`
# and `max_temporary_table_size_bytes_uncompressed` with the settings of the `RESTORE` query.

backup_memory="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_memory')"
backup_mergetree="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_mergetree')"

# Temporary tables live in a session, so everything runs in one client.
$CLICKHOUSE_CLIENT -m -q "
SELECT 'Memory';
CREATE TEMPORARY TABLE tmp_memory (x UInt64) ENGINE = Memory;
INSERT INTO tmp_memory SELECT number FROM numbers(1000000);
BACKUP TEMPORARY TABLE tmp_memory TO ${backup_memory} FORMAT Null;
DROP TEMPORARY TABLE tmp_memory;
RESTORE TEMPORARY TABLE tmp_memory FROM ${backup_memory} SETTINGS max_temporary_table_memory_usage = '1Mi' FORMAT Null; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_memory;
DROP TEMPORARY TABLE tmp_memory;
RESTORE TEMPORARY TABLE tmp_memory FROM ${backup_memory} SETTINGS max_temporary_table_memory_usage = '100Mi' FORMAT Null;
SELECT count() FROM tmp_memory;
DROP TEMPORARY TABLE tmp_memory;

SELECT 'MergeTree';
CREATE TEMPORARY TABLE tmp_mergetree (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x % 4;
INSERT INTO tmp_mergetree SELECT number FROM numbers(1000000);
BACKUP TEMPORARY TABLE tmp_mergetree TO ${backup_mergetree} FORMAT Null;
DROP TEMPORARY TABLE tmp_mergetree;
RESTORE TEMPORARY TABLE tmp_mergetree FROM ${backup_mergetree} SETTINGS max_temporary_table_size_bytes_uncompressed = '1Mi' FORMAT Null; -- { serverError TOO_MANY_BYTES }
-- The restore is rejected as a whole, no part is attached.
SELECT count() FROM tmp_mergetree;
DROP TEMPORARY TABLE tmp_mergetree;
RESTORE TEMPORARY TABLE tmp_mergetree FROM ${backup_mergetree} SETTINGS max_temporary_table_size_bytes_compressed = 1000 FORMAT Null; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_mergetree;
DROP TEMPORARY TABLE tmp_mergetree;
RESTORE TEMPORARY TABLE tmp_mergetree FROM ${backup_mergetree} SETTINGS max_temporary_table_size_bytes_uncompressed = '100Mi' FORMAT Null;
SELECT count() FROM tmp_mergetree;
"
