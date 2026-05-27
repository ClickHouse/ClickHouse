#!/usr/bin/env bash
# Tags: race
# Companion to 04295_105912_modify_column_to_nullable_with_statistics.sql:
# the .sql test exercises the deterministic, non-race code path through
# `canSkipConversionToNullable` for a column with `STATISTICS`. Here we run
# `MODIFY COLUMN ... Nullable(...)` against a table that is being written to
# from several parallel clients, to exercise the same path while the mutation
# is in flight and the table is changing under it. Without the fix in
# `MutateTask.cpp`, a parallel `ALTER DROP COLUMN` racing with the mutation
# could deref a nullptr returned by `tryGet`; with the fix the guard makes
# this path safe.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_105912_conc SYNC"

$CLICKHOUSE_CLIENT -m --query "
    SET allow_statistics = 1;
    SET use_statistics = 1;
    CREATE TABLE t_105912_conc (x UInt32, y UInt8 STATISTICS(tdigest))
    ENGINE = MergeTree ORDER BY x;
"

# Seed with some data so the mutation has something to rewrite.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_105912_conc SELECT number, number % 256 FROM numbers(1000)"

# Background INSERTs running while the mutation is in flight. Bounded by a
# fixed iteration count so the test cannot hang. Stderr is intentionally not
# redirected: any unexpected INSERT failure must surface so the concurrent
# coverage cannot become vacuous.
for i in 1 2 3 4; do
    (
        for j in $(seq 1 20); do
            $CLICKHOUSE_CLIENT --query \
                "INSERT INTO t_105912_conc SELECT number + $i * 100000 + $j * 1000, (number + $j) % 256 FROM numbers(100)"
        done
    ) &
done

# Run the mutation that goes through `canSkipConversionToNullable` for column
# `y`. With `mutations_sync = 2` the ALTER waits for the mutation to apply on
# every active part; under the concurrent inserts above the mutation has to
# process a moving set of parts.
$CLICKHOUSE_CLIENT --query \
    "ALTER TABLE t_105912_conc MODIFY COLUMN y Nullable(UInt8) SETTINGS mutations_sync = 2"

# Drain the background inserts before final checks.
wait

# Mutation succeeded and the new column type is in effect.
$CLICKHOUSE_CLIENT --query \
    "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_105912_conc' AND name = 'y'"

# Sanity check on the row count: 1000 seed rows plus at most 4 * 20 * 100 =
# 8000 from the background loops. We require strictly more than 1000 so at
# least one background INSERT actually completed, keeping the concurrent
# coverage non-vacuous.
$CLICKHOUSE_CLIENT --query \
    "SELECT count() > 1000 AND count() <= 9000 FROM t_105912_conc"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_105912_conc SYNC"
