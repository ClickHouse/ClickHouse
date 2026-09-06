#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# `OPTIMIZE ... DRY RUN` runs a real merge and commits the temporary merged part on the table's
# disks, bypassing the admission-epoch fence that a regular `OPTIMIZE` holds. For a
# `leader_election` table those disks are shared, so the command is rejected outright.
#
# The sibling fail-close contract — `SYSTEM UNFREEZE` refusing a snapshot whose table is not
# attached on this node — cannot be exercised here: `FREEZE` itself is not supported on
# `plain_rewritable`, the only metadata layout `leader_election` currently accepts. That
# rejection is asserted below so that enabling snapshots on this layout does not silently
# leave `SYSTEM UNFREEZE` untested.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_leader_election_dry_run"

# A per-database endpoint (see `04065_leader_election_basic.sh` for why the shared
# `s3_plain_rewritable` disk is not used here).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE test_leader_election_dry_run (x UInt64, s String)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '05045_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/05045_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# Wait until this instance becomes the leader by retrying the INSERT (no fixed sleeps).
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    if $CLICKHOUSE_CLIENT -q "INSERT INTO test_leader_election_dry_run SELECT number, toString(number) FROM numbers(10)" 2>/dev/null; then
        break
    fi
    sleep 1
done
$CLICKHOUSE_CLIENT -q "INSERT INTO test_leader_election_dry_run SELECT number, toString(number) FROM numbers(10, 10)"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_leader_election_dry_run"

# `OPTIMIZE ... DRY RUN` is rejected even here, on the leader.
part_names=$($CLICKHOUSE_CLIENT -q "
    SELECT concat('''', arrayStringConcat(groupArray(name), ''', '''), '''')
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'test_leader_election_dry_run' AND active
    FORMAT TSVRaw")
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE test_leader_election_dry_run DRY RUN PARTS ${part_names}" 2>&1 | grep -o -m1 -F "SUPPORT_IS_DISABLED"

# A regular `OPTIMIZE` on the leader still works, and the data is intact.
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE test_leader_election_dry_run FINAL"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM test_leader_election_dry_run"

# `FREEZE` is not supported on `plain_rewritable` at all (see the comment at the top).
$CLICKHOUSE_CLIENT -q "ALTER TABLE test_leader_election_dry_run FREEZE WITH NAME '05045_le_${CLICKHOUSE_DATABASE}'" 2>&1 | grep -o -m1 -F "SUPPORT_IS_DISABLED"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_leader_election_dry_run"
