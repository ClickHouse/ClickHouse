#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RESTORE ... allow_different_table_def = 1 places a part that was written with byte-based
# granularity onto a table where byte-based granularity is disabled (index_granularity_bytes = 0).
# A row-changing mutation rewrites that part, and must size granules by index_granularity rows
# instead of emitting one mark per row. Both part types are covered: Wide and Compact.
#
# Every granularity-controlling setting is pinned so the mark counts below are exact and the
# stateless-test settings randomizer cannot perturb them.

BACKUP_WIDE="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_wide')"
BACKUP_COMPACT="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_compact')"

##### Wide part #####

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS dst;

    CREATE TABLE src (k UInt64, s String) ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_full_part_storage = 0;
    INSERT INTO src SELECT number, toString(number) FROM numbers(10000);
    -- 10000 rows at index_granularity = 8192: 2 data marks (8192, 1808) + final mark = 3.
    SELECT 'src', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active;
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE src TO ${BACKUP_WIDE}" | grep -o -e BACKUP_CREATED -e BACKUP_FAILED

# min_*_for_wide_part = 0 on the destination only silences an unrelated "settings will be ignored"
# load warning; the destination part comes from RESTORE, not from an insert.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE dst (k UInt64, s String) ORDER BY k SETTINGS index_granularity_bytes = 0, index_granularity = 8192, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE src AS dst FROM ${BACKUP_WIDE} SETTINGS allow_different_table_def = 1" | grep -o -e RESTORED -e RESTORE_FAILED

${CLICKHOUSE_CLIENT} --query "
    SELECT 'dst after restore', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'dst' AND active;

    ALTER TABLE dst DELETE WHERE k = 0 SETTINGS mutations_sync = 2;
    -- 9999 rows must still give 2 data marks (8192, 1807) + final mark = 3, not one mark per row.
    SELECT 'dst after mutation', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'dst' AND active;
    SELECT 'data', count(), sum(k) FROM dst;

    DROP TABLE src;
    DROP TABLE dst;
"

##### Compact part #####

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS csrc;
    DROP TABLE IF EXISTS cdst;

    CREATE TABLE csrc (k UInt64, s String) ORDER BY k
        SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_full_part_storage = 0;
    INSERT INTO csrc SELECT number, toString(number) FROM numbers(10000);
    -- 10000 rows at index_granularity = 8192: 2 marks.
    SELECT 'csrc', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'csrc' AND active;
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE csrc TO ${BACKUP_COMPACT}" | grep -o -e BACKUP_CREATED -e BACKUP_FAILED

${CLICKHOUSE_CLIENT} --query "CREATE TABLE cdst (k UInt64, s String) ORDER BY k SETTINGS index_granularity_bytes = 0, index_granularity = 8192, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE csrc AS cdst FROM ${BACKUP_COMPACT} SETTINGS allow_different_table_def = 1" | grep -o -e RESTORED -e RESTORE_FAILED

${CLICKHOUSE_CLIENT} --query "
    SELECT 'cdst after restore', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'cdst' AND active;

    ALTER TABLE cdst DELETE WHERE k = 0 SETTINGS mutations_sync = 2;
    -- 9999 rows must still give 2 marks, not one mark per row.
    SELECT 'cdst after mutation', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'cdst' AND active;
    SELECT 'data', count(), sum(k) FROM cdst;

    DROP TABLE csrc;
    DROP TABLE cdst;
"
