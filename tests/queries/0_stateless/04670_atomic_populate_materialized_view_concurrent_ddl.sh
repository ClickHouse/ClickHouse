#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-replicated-database - both DDL queries would go through the replicated DDL log, which serializes
#   them by itself, so the race the test sets up cannot happen there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` publishes the view first and subscribes it to its
# source afterwards, under the source's exclusive lock. A concurrent `RENAME` (or `DROP`) of the view in
# between would leave the source subscribed to a view name that no longer exists, and
# `DatabaseCatalog::getReadyDependentViews` treats a single missing dependent as "no views are ready" - so
# inserts into the source would silently stop populating *every* view of that source. The view's DDL guard
# is therefore held until the subscription is registered, which serializes the `RENAME` after the cut.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RENAME_QUERY_ID="rename_04670_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04670 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04670 SELECT number FROM numbers(10);
    CREATE MATERIALIZED VIEW other_04670 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04670;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_subscription"

# Pauses after the view is published, before it is subscribed to the source. Whether this `CREATE`
# succeeds is up to the race with the `RENAME` below - renaming a view away while it is being populated is
# allowed to fail the population - so its result is deliberately not asserted on.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04670 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04670
" > /dev/null 2>&1 &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_subscription PAUSE"

# Has to wait for the view's DDL guard, which the paused `CREATE` holds until the subscription is done.
$CLICKHOUSE_CLIENT --query_id "$RENAME_QUERY_ID" -q "RENAME TABLE mv_04670 TO mv_renamed_04670" > /dev/null 2>&1 &
RENAME_PID=$!

# Give the `RENAME` time to reach the server (and, without the guard, to run to completion inside the
# window) before letting the `CREATE` continue.
for _ in {1..100}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$RENAME_QUERY_ID'")" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_subscription"

wait $CREATE_PID || true
wait $RENAME_PID || true

# The point of the test: the source is not left subscribed to the name the view no longer has, so an insert
# into the source still populates the other view.
$CLICKHOUSE_CLIENT -q "
    SELECT 'source subscribed to the old name:', has(dependencies_table, 'mv_04670')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_04670';
    INSERT INTO src_04670 SELECT number + 10 FROM numbers(5);
    SELECT 'rows in the other view:', count() FROM other_04670;
    DROP TABLE IF EXISTS mv_renamed_04670;
    DROP TABLE IF EXISTS mv_04670;
    DROP TABLE other_04670;
    DROP TABLE src_04670;
"
