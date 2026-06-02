#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses fail points which affect the whole server.
#
# Regression test for https://github.com/ClickHouse/ClickHouse/pull/104822
# inline review on src/Storages/StorageMergeTree.cpp:784.
#
# `StorageMergeTree::prepareMutationEntry` writes a `mutation_*.txt` file to
# disk via `entry.commit` BEFORE `addPreparedMutationEntry` registers the
# entry in `current_mutations_by_version`. If a throw happens between those
# two steps (for example `txn->addMutation` or the in-memory `try_emplace`
# throws), the outer `alter` catch rolls back metadata to `old_metadata` but
# the committed mutation file is left on disk. After a restart,
# `loadMutations` reads the file and re-registers the rename mutation. The
# matching metadata change has already been reverted, so a subsequent part
# mutation runs against the wrong schema and reintroduces the same data-loss
# class on the rollback path.
#
# This test injects a failure in `prepareMutationEntry` right after
# `entry.commit` via a new fail point, runs `ALTER RENAME COLUMN`, expects
# it to throw, then detaches and re-attaches the table to force
# `loadMutations`. Without the fix the orphaned mutation file appears in
# `system.mutations`; with the fix the destructor of
# `MergeTreeMutationEntry` removes the orphan and `loadMutations` finds
# nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_prepared_mutation_cleanup;

    CREATE TABLE t_prepared_mutation_cleanup (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_prepared_mutation_cleanup VALUES (1, 'hello'), (2, 'world');

    SYSTEM ENABLE FAILPOINT mt_throw_after_mutation_commit;
"

# The ALTER must throw because the fail point fires after `entry.commit`
# inside `prepareMutationEntry`. Capture exit status without aborting via set -e.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_prepared_mutation_cleanup RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_throw_after_mutation_commit"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_prepared_mutation_cleanup"
    exit 1
fi

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_throw_after_mutation_commit"

# DETACH + ATTACH forces the storage to be reconstructed, which re-runs
# `loadMutations`. Without the fix, an orphaned `mutation_*.txt` would be
# loaded and appear in `system.mutations` as a not-done rename. With the
# fix, the destructor removed the orphan and there is nothing to load.
$CLICKHOUSE_CLIENT --query="DETACH TABLE t_prepared_mutation_cleanup"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE t_prepared_mutation_cleanup"

orphan_count=$($CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_prepared_mutation_cleanup'
")

if [ "$orphan_count" != "0" ]; then
    echo "FAIL: orphaned mutation persisted across DETACH/ATTACH (count=$orphan_count)"
    $CLICKHOUSE_CLIENT --query="SELECT mutation_id, command FROM system.mutations WHERE database = currentDatabase() AND table = 't_prepared_mutation_cleanup'"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_prepared_mutation_cleanup"
    exit 1
fi
echo "no orphaned mutation after DETACH/ATTACH OK"

# After the failed ALTER and the cleanup, the table still exposes the
# original column `d` and a fresh ALTER must succeed normally.
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_prepared_mutation_cleanup ORDER BY id"

$CLICKHOUSE_CLIENT --query="
    ALTER TABLE t_prepared_mutation_cleanup RENAME COLUMN d TO d1 SETTINGS alter_sync = 2;
    SELECT id, d1 FROM t_prepared_mutation_cleanup ORDER BY id;
    DROP TABLE t_prepared_mutation_cleanup;
"
