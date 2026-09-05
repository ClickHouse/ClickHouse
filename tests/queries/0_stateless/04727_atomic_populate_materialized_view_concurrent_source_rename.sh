#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-replicated-database - both DDL queries would go through the replicated DDL log, which serializes
#   them by itself, so the race the test sets up cannot happen there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` resolves its source table and registers the
# name-keyed subscription afterwards, under the source's exclusive lock. A concurrent `RENAME` (or
# `EXCHANGE`) of the *source* in between would change the owner of the name, so the view would be
# backfilled from one table while the subscription is wired to whatever table owns the name afterwards -
# or to a name nobody owns, so the view would silently receive nothing. The DDL guard of the source's
# name is therefore held across the cut, which serializes source-side DDL after the subscription: the
# `RENAME` then carries the subscription along with the name, and the `EXCHANGE` leaves it with the name,
# exactly as for a pre-existing materialized view.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1) RENAME of the source, issued while the CREATE is inside the cut.

RENAME_QUERY_ID="rename_04727_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04727 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04727 SELECT number FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_subscription"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04727 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04727
" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_subscription PAUSE"

# Has to wait for the source's DDL guard, which the paused `CREATE` holds until the subscription is done.
$CLICKHOUSE_CLIENT --query_id "$RENAME_QUERY_ID" -q "RENAME TABLE src_04727 TO src_renamed_04727" &
RENAME_PID=$!

# Give the `RENAME` time to reach the server (and, without the guard, to run to completion inside the
# window) before letting the `CREATE` continue.
for _ in {1..100}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$RENAME_QUERY_ID'")" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_subscription"

wait $CREATE_PID
wait $RENAME_PID

# The subscription was registered on the table the backfill read, and the `RENAME`, serialized after the
# cut, carried it to the new name - it is not left dangling on the old name. (Whether inserts into the
# renamed source are then delivered to the view is a separate, pre-existing question of renaming any
# materialized view's source - the view's stored SELECT still names the old table - and is not asserted
# here.) Without the source's DDL guard, the subscription would land on the old name, which nobody owns
# now, and a table created later under that name would feed the view even though it was never its source.
$CLICKHOUSE_CLIENT -q "
    SELECT 'renamed source subscribed to the view:', has(dependencies_table, 'mv_04727')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_renamed_04727';
    SELECT 'rows backfilled from the source:', count(), uniqExact(n) FROM mv_04727;
    CREATE TABLE src_04727 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04727 SELECT number + 1000 FROM numbers(5);
    SELECT 'impostor under the old name subscribed to the view:', has(dependencies_table, 'mv_04727')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_04727';
    SELECT 'rows in the view after inserting into the impostor:', count(), uniqExact(n) FROM mv_04727;
    DROP TABLE mv_04727;
    DROP TABLE src_04727;
    DROP TABLE src_renamed_04727;
"

# 2) EXCHANGE of the source with another table, issued while the CREATE is inside the cut.

EXCHANGE_QUERY_ID="exchange_04727_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04727 (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE other_04727 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04727 SELECT number FROM numbers(10);
    INSERT INTO other_04727 SELECT number + 1000 FROM numbers(3);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_subscription"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04727 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04727
" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_subscription PAUSE"

$CLICKHOUSE_CLIENT --query_id "$EXCHANGE_QUERY_ID" -q "EXCHANGE TABLES src_04727 AND other_04727" &
EXCHANGE_PID=$!

for _ in {1..100}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$EXCHANGE_QUERY_ID'")" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_subscription"

wait $CREATE_PID
wait $EXCHANGE_PID

# The backfill read the table that owned the source name at the cut (10 rows), and the `EXCHANGE`, which
# ran only after the cut, left the subscription with the name (see #105021) - so the view now receives
# inserts into the exchanged-in table under the source name, like any pre-existing view would.
$CLICKHOUSE_CLIENT -q "
    SELECT 'exchanged source subscribed to the view:', has(dependencies_table, 'mv_04727')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_04727';
    SELECT 'rows backfilled from the original source:', count(), uniqExact(n) FROM mv_04727;
    INSERT INTO src_04727 SELECT number + 10 FROM numbers(5);
    SELECT 'rows in the view:', count(), uniqExact(n) FROM mv_04727;
    DROP TABLE mv_04727;
    DROP TABLE src_04727;
    DROP TABLE other_04727;
"
