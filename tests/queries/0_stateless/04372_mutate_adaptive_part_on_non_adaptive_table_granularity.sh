#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

# An adaptive part (here a Wide part with .mrk2 marks) can be placed on a table whose
# index_granularity_bytes = 0 (non-adaptive) via RESTORE ... allow_different_table_def = 1.
# A row-changing mutation rewrites the part and inherits the source part's adaptive granularity
# info, so the writer runs with can_use_adaptive = true while index_granularity_bytes = 0. The
# granularity computation must fall back to the fixed row granularity; before the fix it divided
# the zero byte budget by the row size and collapsed every granule to a single row (one mark per row).
#
# Every granularity-controlling setting is pinned so the mark counts are deterministic (they must
# not be perturbed by the stateless-test settings randomizer): src pins index_granularity = 8192 and
# a large index_granularity_bytes so it is sized by rows (2 data marks + final mark = 3); dst pins
# index_granularity = 8192 so the fixed fallback the fix uses is exact.

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS dst;

    CREATE TABLE src (k UInt64, s String) ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_full_part_storage = 0;
    INSERT INTO src SELECT number, toString(number) FROM numbers(10000);
    -- src is a Wide, adaptive part: 10000 rows -> 2 data marks (8192, 1808) + final mark = 3 marks.
    SELECT 'src', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active;
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE src TO ${BACKUP}" | grep -o -e BACKUP_CREATED -e BACKUP_FAILED

# Non-adaptive destination table (index_granularity_bytes = 0), but the restored part stays adaptive.
# min_*_for_wide_part = 0 only silences an unrelated "settings will be ignored" load warning for a
# non-adaptive table; it does not affect this scenario (the destination part comes from RESTORE).
${CLICKHOUSE_CLIENT} --query "CREATE TABLE dst (k UInt64, s String) ORDER BY k SETTINGS index_granularity_bytes = 0, index_granularity = 8192, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE src AS dst FROM ${BACKUP} SETTINGS allow_different_table_def = 1" | grep -o -e RESTORED -e RESTORE_FAILED

${CLICKHOUSE_CLIENT} --query "
    SELECT 'dst after restore', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'dst' AND active;

    ALTER TABLE dst DELETE WHERE k = 0 SETTINGS mutations_sync = 2;
    -- Granularity must NOT collapse to one mark per row. 9999 rows with index_granularity = 8192 give
    -- exactly 2 data marks (8192, 1807) + final mark = 3. Before the fix this was 10000 (one per row).
    SELECT 'dst after mutation', part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 'dst' AND active;
    -- Data stays correct.
    SELECT 'data', count(), sum(k) FROM dst;

    DROP TABLE src;
    DROP TABLE dst;
"
