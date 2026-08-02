#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-replicated-database - both DDL queries would go through the replicated DDL log, which serializes
#   them by itself, so the race the test sets up cannot happen there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` publishes the view first and subscribes it to its
# source afterwards, under the source's exclusive lock. A concurrent `DROP` of the view in between would
# leave the subscription pointing at a view that no longer exists, and `getReadyDependentViews` treats a
# single missing dependent as "no views are ready" - so inserts into the source would silently stop
# populating *every* view of that source. The view's DDL guard is therefore held until the subscription is
# registered, which serializes the concurrent `DROP` after the cut.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DROP_QUERY_ID="drop_04670_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04670 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04670 SELECT number FROM numbers(10);
    CREATE MATERIALIZED VIEW other_04670 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04670;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_subscription"

# Pauses after the view is published, before it is subscribed to the source. Whether this `CREATE` succeeds
# is up to the race with the `DROP` below - dropping a view while it is being populated is allowed to fail
# the population - so its result is deliberately not asserted on.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04670 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04670
" > /dev/null 2>&1 &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_subscription PAUSE"

# Has to wait for the view's DDL guard, which the paused `CREATE` holds until the subscription is done.
$CLICKHOUSE_CLIENT --query_id "$DROP_QUERY_ID" -q "DROP TABLE mv_04670 SYNC" > /dev/null 2>&1 &
DROP_PID=$!

# Give the `DROP` time to reach the server (and, without the guard, to run to completion inside the window)
# before letting the `CREATE` continue.
for _ in {1..100}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$DROP_QUERY_ID'")" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_subscription"

wait $CREATE_PID || true
wait $DROP_PID || true

# The view is gone, and - the point of the test - the source was not left with a subscription naming it, so
# an insert into the source still populates the other view.
$CLICKHOUSE_CLIENT -q "
    SELECT 'dropped view exists:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04670';
    INSERT INTO src_04670 SELECT number + 10 FROM numbers(5);
    SELECT 'rows in the other view:', count() FROM other_04670;
    DROP TABLE other_04670;
    DROP TABLE src_04670;
"
