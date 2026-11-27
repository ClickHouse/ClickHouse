#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-replicated-database - the CREATE would go through the replicated DDL log, where the population is
#   always the legacy one, so the atomic path (and its fail point) is not exercised there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` validates the view's SELECT (resolving the source
# table) before it acquires the DDL guard of the source table's name. A `DROP` of the source landing in
# that window used to slip past: the view was published, `getValidatedAtomicPopulateSource` found no
# source and fell back to the legacy population, whose `INSERT ... SELECT` then failed on the vanished
# name *outside* the rollback scope - the half-created view was left behind and a retry got
# `TABLE_ALREADY_EXISTS`. Now the vanished source fails the CREATE inside the rollback scope: the view
# is dropped, the CREATE fails with `UNKNOWN_TABLE`, and a retry (after re-creating the source) works.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04824 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04824 SELECT number FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_source_guard"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04824 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04824
" 2> /dev/null &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_source_guard PAUSE"

# The paused CREATE holds no DDL guards and no reference to the source (the validation-time storage
# snapshot is released from the query's shared-snapshot cache before the pause point), so the DROP -
# synchronous in the test harness (`database_atomic_wait_for_drop_and_detach_synchronously`), meaning it
# waits for every reference to the storage to be released - runs to completion inside the window.
$CLICKHOUSE_CLIENT -q "DROP TABLE src_04824"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_source_guard"

wait $CREATE_PID
CREATE_STATUS=$?

# The CREATE failed on the vanished source, and the rollback dropped the just-published view, so
# nothing of what the failed CREATE created is left behind.
echo "create failed: $((CREATE_STATUS != 0))"
$CLICKHOUSE_CLIENT -q "
    SELECT 'view left behind:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04824';
"

# A retry after re-creating the source succeeds (an earlier revision failed here with TABLE_ALREADY_EXISTS).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04824 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04824 SELECT number FROM numbers(10);
    CREATE MATERIALIZED VIEW mv_04824 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04824;
    SELECT 'retry succeeded, rows backfilled:', count(), uniqExact(n) FROM mv_04824;
    SELECT 'retried view subscribed to the source:', has(dependencies_table, 'mv_04824')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_04824';
    DROP TABLE mv_04824;
    DROP TABLE src_04824;
"
