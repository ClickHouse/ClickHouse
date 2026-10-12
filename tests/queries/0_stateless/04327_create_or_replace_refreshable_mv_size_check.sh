#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree
# ^ refreshable non-APPEND MV requires Atomic database; the inner-name shape
#   `.inner_id.<uuid>` is a non-`fixed_uuid` codepath that does not exist on
#   Replicated/Shared databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Refreshable, non-`APPEND` materialized view exercises the `!fixed_uuid`
# branch of `StorageMaterializedView::checkTableSizeBelowDropLimit`. Its inner
# table is named `.inner_id.<uuid>` (Atomic database adds the UUID), and
# `dropInnerTableIfAny` builds `to_drop` from BOTH that live inner AND a
# `.tmp.inner_id.<uuid>` shadow. The pre-flight has to forward through both;
# this test pins the live-inner branch (the `.tmp` shadow is only observable
# while a refresh is in flight, hard to trigger deterministically here).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t04327_src SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t04327_mv SYNC"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t04327_src (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t04327_src SELECT number FROM numbers(1000)"

# `REFRESH EVERY 100 YEAR` keeps `fixed_uuid = false` and pushes the next
# refresh out far enough that it cannot race with this test.
${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE MATERIALIZED VIEW t04327_mv
        REFRESH EVERY 100 YEAR
        ENGINE = MergeTree ORDER BY id
        AS SELECT id FROM t04327_src
"

# Force the initial refresh synchronously so the live inner has data.
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH VIEW t04327_mv"
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT VIEW t04327_mv"

# `CREATE OR REPLACE MATERIALIZED VIEW` over a refreshable non-`APPEND` MV must
# observe the live inner's size and refuse before EXCHANGE. Plain `DROP TABLE`
# on a refreshable MV with an over-limit inner is refused (see
# `03667_drop_inner_table_size_limits.sql`); replace must not be a privileged
# path that bypasses the same guard.
${CLICKHOUSE_CLIENT} --max_table_size_to_drop 1 -q "
    CREATE OR REPLACE MATERIALIZED VIEW t04327_mv
        REFRESH EVERY 200 YEAR
        ENGINE = MergeTree ORDER BY id
        AS SELECT id FROM t04327_src
" 2>&1 | grep -F "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT" >/dev/null && echo "rejected" || echo "passed unexpectedly"

# The original view and its inner must still be there.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t04327_mv"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%'"

${CLICKHOUSE_CLIENT} --max_table_size_to_drop 0 -q "DROP TABLE t04327_mv SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t04327_src SYNC"
