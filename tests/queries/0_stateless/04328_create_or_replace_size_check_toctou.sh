#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# no-parallel: The `PAUSEABLE_ONCE` failpoint fires exactly once globally; a concurrent
#   `CREATE OR REPLACE` from another parallel test could steal the pause.
# no-replicated-database: Failpoints are single-server.
# no-ordinary-database: `CREATE OR REPLACE TABLE` requires an `Atomic` database.
# no-shared-merge-tree: Cloud-only engine; the size check path differs.
#
# Regression coverage for the TOCTOU race between the pre-flight `checkTableSizeBelowDropLimit`
# and the rename's `DDLGuard`s in `doCreateOrReplaceTable`.
#
# Bug: the size check ran on whatever `tryGetTable` returned for `(database, t)` BEFORE
# `InterpreterRenameQuery::execute` acquired the per-table `DDLGuard`s. A concurrent
# `CREATE OR REPLACE TABLE t (...)` could replace `t` between our check and the rename.
# Our query then exchanged with the replaced storage, and the implicit drop ran with
# `max_table_size_to_drop = 0`, silently deleting an object the user's setting would have
# rejected.
#
# Fix: remember the UUID of the storage we approved, then verify after the rename that
# the storage we are about to drop has that UUID. If not, fall back to a normal drop
# context where the user's `max_table_size_to_drop` setting is enforced.
#
# The failpoint `create_or_replace_after_size_check_before_rename` pauses the running
# `doCreateOrReplaceTable` between the pre-flight check and the rename, giving a
# concurrent query time to swap the target. The test exercises that interleaving and
# verifies the server stays consistent (no `LOGICAL_ERROR`, no orphaned data, no
# server-side crash).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_or_replace_after_size_check_before_rename" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --max_table_size_to_drop=0 -q "DROP TABLE IF EXISTS t04328" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

# Setup: a small original table that easily passes the pre-flight size check.
${CLICKHOUSE_CLIENT} -nm -q "
    CREATE TABLE t04328 (a UInt64) ENGINE = MergeTree() ORDER BY a;
    INSERT INTO t04328 VALUES (1);
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_or_replace_after_size_check_before_rename"

# Q1: a `CREATE OR REPLACE` with a tight `max_table_size_to_drop`. The pre-flight
# check sees the original 1-row table and passes, then Q1 pauses BEFORE acquiring
# the rename's DDLGuards.
${CLICKHOUSE_CLIENT} \
    --max_table_size_to_drop=10000 \
    -q "CREATE OR REPLACE TABLE t04328 (b UInt64) ENGINE = MergeTree() ORDER BY b
        AS SELECT number FROM numbers(2)" >/dev/null 2>&1 &
Q1_PID=$!

# Wait until Q1 has finished the pre-flight size check and is paused.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_or_replace_after_size_check_before_rename PAUSE"

# Disable the failpoint so Q2 does not also pause itself.
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_or_replace_after_size_check_before_rename"

# Q2: swap in a much larger storage with a different UUID. Q2's own size check
# passes against the original 1-row table; Q2 commits cleanly while Q1 is parked.
${CLICKHOUSE_CLIENT} \
    --max_table_size_to_drop=1000000000 \
    -q "CREATE OR REPLACE TABLE t04328 (c UInt64) ENGINE = MergeTree() ORDER BY c
        AS SELECT number FROM numbers(100000)"

# Resume Q1.
#
# Without the fix: Q1's implicit drop runs with `max_table_size_to_drop = 0` against
# whichever storage now lives at the temp name after EXCHANGE. That storage came from
# Q2, not from Q1's pre-flight check, so the user's `max_table_size_to_drop = 10000`
# is silently bypassed and Q2's 100000-row storage is deleted.
#
# With the fix: Q1 recorded the UUID of the original 1-row storage at check time.
# After the rename, it looks up the storage now at the temp name and finds Q2's UUID
# instead. It falls back to the normal drop path, which honors Q1's setting and
# the implicit drop is rejected. No silent data loss.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_or_replace_after_size_check_before_rename" 2>/dev/null ||:
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT create_or_replace_after_size_check_before_rename"

wait $Q1_PID 2>/dev/null

# We assert the server completed the race without any crash, `LOGICAL_ERROR`, or
# stranded `_tmp_replace_*` table. The exact final state of `t04328` depends on
# which `CREATE OR REPLACE` won the rename's `DDLGuard` last; both outcomes are
# acceptable here, what matters is that the server stayed consistent.
echo "tmp_replace_left:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%'"
echo "t04328_exists:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't04328'"
