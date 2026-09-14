#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

# A merge must not fail when every column of a part was renamed by a mutation that has not been
# applied yet. `injectRequiredColumns` maps the part's column names through `alter_conversions`
# before matching them against the metadata, so the old names on disk are recognised as the columns
# the structure knows under their new names.
#
# The bug triggers when the merge reads a column that has no physical file in the part (added after
# insertion with a constant DEFAULT). Its default has no column dependencies, so
# `have_at_least_one_physical_column` stays false and the part is examined on its own. Before the
# fix that meant injecting the part's minimum-size column, and every part column was filtered out of
# the candidate set because the part only knows the old names -- the empty-list fallback restored
# them and the injected old name could not be resolved against the metadata:
# `NO_SUCH_COLUMN_IN_TABLE`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS data_rename" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS data_rename;

CREATE TABLE data_rename (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    -- Packed part storage reports no per-column sizes, which changed which column the old code
    -- injected and hid the bug. Keep the parts in full storage so the affected path is always taken.
    min_bytes_for_full_part_storage = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

-- Insert enough rows so that \`h\` (UInt8) is clearly the smallest column by compressed size.
INSERT INTO data_rename SELECT number, 0 FROM numbers(10000);
INSERT INTO data_rename SELECT number + 10000, 0 FROM numbers(10000);

-- Add a column that will NOT be physically present in existing parts. Its default is a constant,
-- so it has no column dependencies.
ALTER TABLE data_rename ADD COLUMN value UInt64 DEFAULT 42;

-- Prevent the background thread from applying the mutation.
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

-- Rename every column with alter_sync = 0 so the ALTER returns immediately. The mutation is queued
-- but not applied — the parts still hold the old names on disk.
SET alter_sync = 0;
ALTER TABLE data_rename RENAME COLUMN a TO c, RENAME COLUMN h TO d;

-- Without the fix this fails with \`NO_SUCH_COLUMN_IN_TABLE\`, which is what this test guards.
OPTIMIZE TABLE data_rename FINAL;

-- Asserted while the failpoint still holds the mutation back, so the state cannot change underneath.
-- Only the row count and the column that was not renamed are checked. What a merged part reports for
-- \`c\` and \`d\` while the RENAME is still pending is governed by a separate, pre-existing problem
-- (such a part stores the new names but keeps a data version below the rename mutation, so the
-- rename is applied a second time on read). Asserting it here would tie this test to it.
SELECT count(), min(value), max(value) FROM data_rename;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"
