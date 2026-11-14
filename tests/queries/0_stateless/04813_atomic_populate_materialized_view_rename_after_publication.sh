#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-replicated-database - the CREATE would go through the replicated DDL log, where the population is
#   always the legacy one, so the atomic path (and its fail point) is not exercised there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` holds the DDL guard of its source table's *name*
# from before the view is published (created in the catalog) until the subscription cut is done. The
# view's name here deliberately sorts *before* the source's: in the canonical ascending guard-acquisition
# order the view's guard is then taken first, and an earlier revision acquired the source's guard only
# after the publication in this ordering. A `RENAME` of the source landing in that gap made the CREATE
# fall back to the legacy population, which failed with `UNKNOWN_TABLE` and left the just-published view
# behind. Now both guards are held before the publication, so the `RENAME`, issued while the CREATE is
# paused right after the publication, must wait for the cut: the population reads the source under its
# original name and the subscription is carried along by the rename afterwards.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RENAME_QUERY_ID="rename_04813_${CLICKHOUSE_DATABASE}"

# 'mv_...' < 'src_...', so the view's name sorts first.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_04813 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_04813 SELECT number FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_after_view_publication"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW mv_04813 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04813
" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_after_view_publication PAUSE"

# Has to wait for the source's DDL guard, which the paused `CREATE` already holds.
$CLICKHOUSE_CLIENT --query_id "$RENAME_QUERY_ID" -q "RENAME TABLE src_04813 TO src_renamed_04813" &
RENAME_PID=$!

# Give the `RENAME` time to reach the server (and, without the guard, to run to completion inside the
# window) before letting the `CREATE` continue.
for _ in {1..100}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$RENAME_QUERY_ID'")" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_after_view_publication"

wait $CREATE_PID
CREATE_STATUS=$?
wait $RENAME_PID

# The CREATE stayed on the atomic path and succeeded (an earlier revision failed with UNKNOWN_TABLE
# here), the backfill read the full source, and the rename, serialized after the cut, carried the
# subscription to the new name.
echo "create succeeded: $CREATE_STATUS"
$CLICKHOUSE_CLIENT -q "
    SELECT 'view exists:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04813';
    SELECT 'rows backfilled from the source:', count(), uniqExact(n) FROM mv_04813;
    SELECT 'renamed source subscribed to the view:', has(dependencies_table, 'mv_04813')
        FROM system.tables WHERE database = currentDatabase() AND name = 'src_renamed_04813';
    DROP TABLE mv_04813;
    DROP TABLE src_renamed_04813;
"
