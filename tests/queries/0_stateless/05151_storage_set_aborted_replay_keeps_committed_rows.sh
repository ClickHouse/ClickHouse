#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would break the persistent `Set` inserts of
# concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Set` that fails while replaying its promoted backup must publish
# nothing at all: the replay builds a private `Set`, so the live one is never touched and the
# `SetPtr` that a running query captured before the insert keeps describing the committed rows.
# The backup of the failed insert must be removed as well, or a restart would restore its rows.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS set_failed_replay;
    CREATE TABLE set_failed_replay (k UInt64) ENGINE = Set SETTINGS persistent = 1;
    INSERT INTO set_failed_replay VALUES (1);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_fail_during_backup_replay"

$CLICKHOUSE_CLIENT --query "INSERT INTO set_failed_replay VALUES (2), (3)" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_fail_during_backup_replay"

echo "matching rows after the failed insert:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_failed_replay"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_failed_replay;
    ATTACH TABLE set_failed_replay;
"
echo "matching rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_failed_replay"

# The table is still usable afterwards.
$CLICKHOUSE_CLIENT --query "INSERT INTO set_failed_replay VALUES (4)"
echo "matching rows after a successful insert:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_failed_replay"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_failed_replay;
    ATTACH TABLE set_failed_replay;
"
echo "matching rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(10) WHERE number IN set_failed_replay"

$CLICKHOUSE_CLIENT --query "DROP TABLE set_failed_replay"
