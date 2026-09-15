#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would pause the persistent `Set` inserts of
# concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Set` promotes its staged backup and then replays it. `getSet`
# hands out a bare `SetPtr` and `Set::insertFromBlock` only takes the per-call `Set::rwlock`, so
# replaying into the live `Set` would let a concurrent `... IN set_table` query observe the rows of
# an insert that is not published yet -- and keep observing them even if the insert later fails.
# The replay therefore builds a private `Set` and publishes it with a single swap: while it is in
# progress, queries must still see exactly the previously committed rows.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS set_mid_replay;
    CREATE TABLE set_mid_replay (k UInt64) ENGINE = Set SETTINGS persistent = 1;
    INSERT INTO set_mid_replay VALUES (1);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_pause_during_backup_replay"

$CLICKHOUSE_CLIENT --query "INSERT INTO set_mid_replay VALUES (2), (3)" &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT storage_set_pause_during_backup_replay PAUSE"

echo "matching rows while the replay is in progress:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_mid_replay"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_pause_during_backup_replay"

wait $insert_pid

echo "matching rows after the insert finished:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_mid_replay"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_mid_replay;
    ATTACH TABLE set_mid_replay;
"
echo "matching rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_mid_replay"

$CLICKHOUSE_CLIENT --query "DROP TABLE set_mid_replay"
