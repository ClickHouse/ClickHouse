#!/usr/bin/env bash
# Tags: atomic-database
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104900
#
# A Refreshable MaterializedView's previous inner target table is renamed to
# `.tmp.inner_id.<uuid>` by `exchangeTargetTable` and then dropped by the refresh task
# itself in `dropTempTable`. When the user has lowered `max_table_size_to_drop` below
# the size of the rotated-out table, the drop used to fail and the temporary table
# would be leaked. Every subsequent refresh would create yet another fresh inner table
# while still being unable to drop the leaked ones, growing the view's data directory
# until the disk fills.
#
# The fix bypasses `max_table_size_to_drop` / `max_partition_size_to_drop` for the
# refresh-task drop — that safety check exists to protect users from accidentally
# DROPping large tables, but the temporary inner here was created by the refresh task
# itself and the user is not in control of its size.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rmv_104900 SYNC;
    DROP TABLE IF EXISTS src_104900 SYNC;

    CREATE TABLE src_104900 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_104900 SELECT number FROM numbers(1000);

    -- The MV's SELECT pins \`max_table_size_to_drop = 1\` so the refresh context
    -- inherits it (via \`applySettingsFromQuery\` in \`prepareRefresh\`). The MV's
    -- target table after the first refresh holds 1000 UInt64 rows — well over 1 byte —
    -- so dropping it during the second refresh's rotation requires the fix.
    CREATE MATERIALIZED VIEW rmv_104900
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY
        AS SELECT * FROM src_104900 SETTINGS max_table_size_to_drop = 1;
"

# Refresh #1: the previous target is the initial empty inner table created by
# \`CREATE MATERIALIZED VIEW\`. Empty MergeTree has 0 active bytes, so dropping it
# does not trip the size check (0 <= 1). After this, the new target holds the data.
$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW rmv_104900;
    SYSTEM WAIT VIEW rmv_104900;
"

# Refresh #2: the previous target now holds the 1000-row MergeTree. Without the fix,
# dropping it would throw TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT and leak the
# \`.tmp.inner_id.<uuid>\` table. With the fix, the refresh-task drop bypasses the
# size safety net and cleans the rotated-out table.
$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW rmv_104900;
    SYSTEM WAIT VIEW rmv_104900;
"

# Expect 0 leaked tmp inner tables. The view's own inner (.inner_id.<uuid>) still
# exists — that's the live target — but no .tmp.inner_id.* should remain.
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(name LIKE '.tmp.%') FROM system.tables WHERE database = currentDatabase();
    SELECT count() FROM rmv_104900;
"

# Cleanup uses a fresh client context where \`max_table_size_to_drop\` is unset, so
# these final drops are unaffected by the in-view setting.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE rmv_104900 SYNC;
    DROP TABLE src_104900 SYNC;
"
