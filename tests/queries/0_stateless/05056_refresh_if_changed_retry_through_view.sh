#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# `REFRESH ... IF CHANGED` reading through a `View`: a refresh that succeeded only on a retry must
# still leave a usable watermark. The source hash of a view must not depend on per-attempt
# diagnostics (`log_comment` carries the attempt number, and it reaches
# `StorageView::getModificationHash` through the refresh context), otherwise the next scheduled
# attempt recomputes a different source hash and an `APPEND` view appends a duplicate copy of
# unchanged rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    -- The refresh reads the source through a view, so the source hash goes through
    -- StorageView::getModificationHash rather than the table's own hash.
    CREATE VIEW v AS SELECT x FROM src;
    -- APPEND mode: every refresh that actually runs appends one row to the view.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED
        -- A long retry backoff: the retry that succeeds must still be a retry. With a short backoff
        -- all 101 attempts fail while the source table is detached, and the refresh gives up and
        -- moves to the next timeslot, where the attempt counter starts from one again.
        SETTINGS refresh_retries = 100, refresh_retry_initial_backoff_ms = 10000, refresh_retry_max_backoff_ms = 10000
        APPEND ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM v;
"

# Wait for the first refresh (it always runs, there is no previous state to compare to).
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done
[ "$n" -ge 1 ] && echo "first refresh ran: yes" || echo "first refresh ran: no ($n)"

# Make the next refresh fail on its first attempt: the source table is temporarily gone.
# Stop the refreshes first, so that the change of the source cannot be picked up before the DETACH.
$CLICKHOUSE_CLIENT -q "
    SYSTEM STOP VIEW mv;
    INSERT INTO src VALUES (2);
    DETACH TABLE src;
    SYSTEM START VIEW mv;
"

# Wait until at least one attempt has failed, then let the retry succeed.
for _ in {1..600}
do
    retries=$($CLICKHOUSE_CLIENT -q "SELECT retry FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv'")
    [ "$retries" -ge 1 ] && break
    sleep 0.1
done
[ "$retries" -ge 1 ] && echo "attempt failed: yes" || echo "attempt failed: no ($retries)"

$CLICKHOUSE_CLIENT -q "ATTACH TABLE src"

# The retry must append exactly one row for the changed source.
for _ in {1..240}
do
    n2=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n2" -ge 2 ] && break
    sleep 0.5
done
[ "$n2" = "2" ] && echo "retry appended one row: yes" || echo "retry appended one row: no ($n2)"

# The source is unchanged since that retry, so all subsequent scheduled refreshes must be skipped
# even though the successful refresh was a retry and this one is a first attempt.
sleep 5
n3=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$n3" = "2" ] && echo "watermark survived the retry: yes" || echo "watermark survived the retry: no ($n2 -> $n3)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP VIEW v SYNC;
    DROP TABLE src SYNC;
"
