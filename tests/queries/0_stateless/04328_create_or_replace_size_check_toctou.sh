#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# no-parallel: The `PAUSEABLE_ONCE` failpoint fires exactly once globally; a concurrent
#   `CREATE OR REPLACE` from another parallel test could steal the pause.
# no-replicated-database: Failpoints are single-server.
# no-ordinary-database: `CREATE OR REPLACE TABLE` requires an `Atomic` database.
# no-shared-merge-tree: Cloud-only engine; the size check path differs.
#
# Regression coverage for the `TOCTOU` race in `doCreateOrReplaceTable`: a concurrent
# `CREATE OR REPLACE` could swap in over-limit storage between picking the target and the
# `EXCHANGE`. The fix gates the swap on a `checkTableSizeBelowDropLimit` invoked from inside
# `InterpreterRenameQuery` while the per-table `DDLGuard`s are held (see `setPreSwapCheck`),
# so the exchange against over-limit storage is refused atomically (no rename happens, no
# stranded `_tmp_replace_*`).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_or_replace_before_rename" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --max_table_size_to_drop=0 -q "DROP TABLE IF EXISTS t04328" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

# Setup: a small original table that easily passes the size check.
${CLICKHOUSE_CLIENT} -nm -q "
    CREATE TABLE t04328 (a UInt64) ENGINE = MergeTree() ORDER BY a;
    INSERT INTO t04328 VALUES (1);
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_or_replace_before_rename"

# Q1: a `CREATE OR REPLACE` with a tight `max_table_size_to_drop`. It fills its temp
# table, then pauses BEFORE acquiring the rename's `DDLGuard`s (the in-rename size
# check has not run yet). We capture stderr to assert Q1 fails with the expected
# size-limit error after Q2 swaps in over-limit storage.
Q1_LOG=$(mktemp)
${CLICKHOUSE_CLIENT} \
    --max_table_size_to_drop=10000 \
    -q "CREATE OR REPLACE TABLE t04328 (b UInt64) ENGINE = MergeTree() ORDER BY b
        AS SELECT number FROM numbers(2)" >/dev/null 2>"$Q1_LOG" &
Q1_PID=$!

# Wait until Q1 has filled its temp table and is paused before the rename.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_or_replace_before_rename PAUSE"

# `PAUSEABLE_ONCE` self-disables after the first hit, so Q2 below will not pause
# at the same failpoint, so leave it enabled so Q1 stays parked.

# Q2: swap in a much larger storage with a different UUID. Q2's own size check
# passes against the original 1-row table; Q2 commits cleanly while Q1 is parked.
${CLICKHOUSE_CLIENT} \
    --max_table_size_to_drop=1000000000 \
    -q "CREATE OR REPLACE TABLE t04328 (c UInt64) ENGINE = MergeTree() ORDER BY c
        AS SELECT number FROM numbers(100000)"

# Resume Q1. The in-rename `pre_swap_check` runs `checkTableSizeBelowDropLimit` on
# whatever now lives at `t04328` while holding the rename's `DDLGuard`s. It sees
# Q2's 100000-row storage, well above Q1's `max_table_size_to_drop=10000`, and
# throws. The exception propagates back to `doCreateOrReplaceTable` with
# `renamed=false`, so the catch block drops Q1's filled temporary table. No
# `EXCHANGE` happens; `t04328` keeps Q2's storage; no `_tmp_replace_*` leaks.
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT create_or_replace_before_rename"

wait $Q1_PID 2>/dev/null

# Q1 must fail with `TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT` (error code 359), not a
# `LOGICAL_ERROR` or any other shape. Echo a normalized marker; we strip the cgroup-
# specific path that some message variants embed.
echo "q1_failed_as_expected:"
if grep -q "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT\|Table or Partition .* was not dropped" "$Q1_LOG"; then
    echo "1"
else
    echo "0"
    echo "--- Q1 stderr was: ---"
    cat "$Q1_LOG"
fi
rm -f "$Q1_LOG"

# Final state: `t04328` exists (Q2's storage), no orphaned `_tmp_replace_*`.
echo "tmp_replace_left:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%'"
echo "t04328_exists:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't04328'"
echo "t04328_row_count:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t04328"
