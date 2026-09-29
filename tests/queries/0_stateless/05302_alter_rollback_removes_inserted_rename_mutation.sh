#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints make the alter's in-memory commit throw.
# no-replicated-database -- this exercises the non-replicated durable rollback, and `DETACH TABLE` is refused there.
# no-shared-merge-tree -- the rollback under test is `StorageMergeTree::alter`'s.

# A failed in-memory commit of `RENAME COLUMN` is rolled back whether it threw before or after the rename mutation
# was inserted: nothing is left in `system.mutations` or reloaded by `DETACH`/`ATTACH`, and the next rename succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

THROW_BEFORE_INSERT_FP="mt_alter_throw_in_start_mutation"
THROW_AFTER_INSERT_FP="mt_throw_after_mutation_entry_inserted"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $THROW_BEFORE_INSERT_FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $THROW_AFTER_INSERT_FP" 2>/dev/null
}
trap cleanup EXIT

function mutation_count()
{
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't'"
}

function run_arm()
{
    local failpoint=$1
    echo "arm: $failpoint"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS t;
        CREATE TABLE t (id UInt64, d String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
        INSERT INTO t VALUES (1, 'a');
        INSERT INTO t VALUES (2, 'b');
        SYSTEM ENABLE FAILPOINT $failpoint;"

    $CLICKHOUSE_CLIENT --query "ALTER TABLE t RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" 2>&1 \
        | expect_error "FAULT_INJECTED" "Injected failure"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint"

    echo "mutations after rollback: $(mutation_count)"
    $CLICKHOUSE_CLIENT --query "DETACH TABLE t"
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE t"
    echo "mutations after reload: $(mutation_count)"

    $CLICKHOUSE_CLIENT --query "
        SELECT id, d FROM t ORDER BY id;
        ALTER TABLE t RENAME COLUMN d TO d1 SETTINGS alter_sync = 2;
        OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1;
        SELECT id, d1 FROM t ORDER BY id;
        SELECT 'active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
        SELECT 'unfinished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND NOT is_done;
        DROP TABLE t;"
}

run_arm "$THROW_BEFORE_INSERT_FP"
run_arm "$THROW_AFTER_INSERT_FP"
