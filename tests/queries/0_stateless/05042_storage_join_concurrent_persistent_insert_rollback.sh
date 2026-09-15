#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would pause the persistent `Set`/`Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two concurrent persistent inserts into a `Join` table, one of which fails while replaying its
# promoted backup (it exceeds `max_rows_in_join`). The failed insert rolls the live state back by
# rebuilding it from the committed backups, and that rollback must be serialized with the replay
# of the surviving insert: without the serialization the surviving insert could apply its rows on
# top of a rebuilt state that already contains them, exposing them twice.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_concurrent;
    CREATE TABLE join_concurrent (k UInt64, v String) ENGINE = Join(ALL, LEFT, k)
        SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
    INSERT INTO join_concurrent VALUES (1, 'committed');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_replay"

# The survivor promotes its backup file, then pauses before replaying it into the live state,
# inside the publish critical section.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_concurrent VALUES (2, 'survivor')" &
survivor_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_replay PAUSE"

# This insert makes the table exceed `max_rows_in_join`, so its replay throws after its backup
# has been promoted, and the rollback rebuilds the live state from the committed backups. It
# queues behind the paused survivor, so the survivor always publishes first.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_concurrent VALUES (3, 'over_limit')" 2>&1 | grep -o 'SET_SIZE_LIMIT_EXCEEDED' | head -1 &
failing_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_replay"

wait $survivor_pid
wait $failing_pid

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_concurrent ORDER BY k"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_concurrent;
    ATTACH TABLE join_concurrent;
"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_concurrent ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_concurrent"
