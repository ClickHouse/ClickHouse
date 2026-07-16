#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses fail points which affect the whole server.
#
# Regression test for https://github.com/ClickHouse/ClickHouse/pull/104822
# inline review (error-path rollback gap).
#
# In `StorageMergeTree::alter`, `setProperties` publishes the new in-memory
# metadata before `startMutation` registers the rename mutation. If
# `startMutation` throws after that point, the catch block must revert the
# in-memory metadata back to `old_metadata` (under
# `currently_processing_in_background_mutex`); otherwise readers see the new
# column names while the on-disk parts still have the old ones, and the
# renamed column reads as default until restart.
#
# This test injects a failure in `startMutation` via a failpoint, runs
# `ALTER RENAME COLUMN`, expects it to throw, and then verifies that the
# table still serves the original column (in-memory metadata reverted).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_rename_rollback;

    CREATE TABLE t_rename_rollback (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_rename_rollback VALUES (1, 'hello'), (2, 'world');

    SYSTEM ENABLE FAILPOINT mt_alter_throw_in_start_mutation;
"

# The ALTER must throw. Capture exit status without aborting via set -e.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_rename_rollback RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_in_start_mutation"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_rollback"
    exit 1
fi

# After the failed ALTER, the old column `d` must still be queryable. If the
# in-memory rollback was missing, the table would expose column `d1` and
# `SELECT d` would fail with UNKNOWN_IDENTIFIER.
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_rename_rollback ORDER BY id"

# The new column must NOT exist (rollback succeeded).
set +e
$CLICKHOUSE_CLIENT --query="SELECT d1 FROM t_rename_rollback" 2>/dev/null
select_d1_status=$?
set -e
if [ "$select_d1_status" -eq 0 ]; then
    echo "FAIL: column 'd1' is visible after rolled-back ALTER"
    $CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_in_start_mutation"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_rollback"
    exit 1
fi
echo "d1 not visible OK"

# A subsequent ALTER (with the failpoint disabled) must succeed and rename
# the column normally.
$CLICKHOUSE_CLIENT --query="
    SYSTEM DISABLE FAILPOINT mt_alter_throw_in_start_mutation;
    ALTER TABLE t_rename_rollback RENAME COLUMN d TO d1 SETTINGS alter_sync = 2;
    SELECT id, d1 FROM t_rename_rollback ORDER BY id;
    DROP TABLE t_rename_rollback;
"
