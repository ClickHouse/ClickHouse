#!/usr/bin/env bash
# Tags: atomic-database
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104900

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Scenario A: the rotated-out target table is dropped by the refresh task in
# dropTempTable. With max_table_size_to_drop lowered below its size, that drop used to
# fail and leak the .tmp.inner_id.<uuid> table; the fix bypasses the size limits there.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rmv_104900 SYNC;
    DROP TABLE IF EXISTS src_104900 SYNC;

    CREATE TABLE src_104900 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_104900 SELECT number FROM numbers(1000);

    CREATE MATERIALIZED VIEW rmv_104900
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY
        AS SELECT * FROM src_104900 SETTINGS max_table_size_to_drop = 1;
"

# Refresh #1: previous target is the initial empty inner table (0 bytes, drops fine).
# Refresh #2: previous target holds 1000 rows; the refresh-task drop must bypass the
# size limit, otherwise the .tmp.inner_id.<uuid> table is leaked.
$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW rmv_104900;
    SYSTEM WAIT VIEW rmv_104900;
    SYSTEM REFRESH VIEW rmv_104900;
    SYSTEM WAIT VIEW rmv_104900;
"

# Expect 0 leaked tmp inner tables and the data in place.
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(name LIKE '.tmp.%') FROM system.tables WHERE database = currentDatabase();
    SELECT count() FROM rmv_104900;
"

# Scenario B: a leftover .tmp.inner_id.<uuid> from a previous failed refresh exists when
# a refresh starts. CREATE OR REPLACE renames the leftover to a _tmp_replace_<random> name
# and drops it; that internal drop used to honor max_table_size_to_drop, so a large leftover
# failed to drop and leaked under that name. The fix runs CREATE OR REPLACE on a context that
# bypasses the size limits, so the leftover is dropped instead of leaked.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rmv2_104900 SYNC;
    DROP TABLE IF EXISTS src2_104900 SYNC;

    CREATE TABLE src2_104900 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src2_104900 SELECT number FROM numbers(1000);

    CREATE MATERIALIZED VIEW rmv2_104900
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY
        AS SELECT * FROM src2_104900 SETTINGS max_table_size_to_drop = 1;
"

# Plant a large leftover at the exact name the next refresh will reuse:
# .tmp.inner_id.<view_uuid>, holding 1000 rows (well over the 1-byte drop limit).
view_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'rmv2_104900'")
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE \`.tmp.inner_id.${view_uuid}\` (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`.tmp.inner_id.${view_uuid}\` SELECT number FROM numbers(1000);
    SYSTEM REFRESH VIEW rmv2_104900;
    SYSTEM WAIT VIEW rmv2_104900;
"

# Expect 0 leaked tmp inner tables and 0 _tmp_replace_* tables, and the data in place.
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(name LIKE '.tmp.%') + countIf(name LIKE '%_tmp_replace_%') FROM system.tables WHERE database = currentDatabase();
    SELECT count() FROM rmv2_104900;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rmv_104900 SYNC;
    DROP TABLE src_104900 SYNC;
    DROP TABLE rmv2_104900 SYNC;
    DROP TABLE src2_104900 SYNC;
"
