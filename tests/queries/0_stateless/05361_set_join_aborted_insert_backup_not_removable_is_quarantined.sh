#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses server-wide failpoints that would break the persistent `Set` and `Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Set` that fails after its backup was promoted must not leave that
# backup to be replayed, even if the backup cannot be removed: it is moved back to `tmp/`, which is
# never replayed, so neither the next successful `INSERT` nor a reattach brings its rows back. If
# even that move fails, the backup is excluded from the replays of later inserts.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS set_quarantine;
    CREATE TABLE set_quarantine (k UInt64) ENGINE = Set SETTINGS persistent = 1;
    INSERT INTO set_quarantine VALUES (1);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_fail_removing_promoted_backup"
$CLICKHOUSE_CLIENT --query "INSERT INTO set_quarantine VALUES (2), (3)" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_fail_during_backup_replay"

# The successful insert replays every committed backup into a new `Set`.
$CLICKHOUSE_CLIENT --query "INSERT INTO set_quarantine VALUES (4)"
echo "rows after the next insert:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_quarantine"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_quarantine;
    ATTACH TABLE set_quarantine;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_quarantine"

# The backup can be neither removed nor moved away.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_or_join_fail_quarantining_backup"
$CLICKHOUSE_CLIENT --query "INSERT INTO set_quarantine VALUES (5)" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_or_join_fail_quarantining_backup"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_fail_removing_promoted_backup"

$CLICKHOUSE_CLIENT --query "INSERT INTO set_quarantine VALUES (6)"
echo "rows after the next insert:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_quarantine"

$CLICKHOUSE_CLIENT --query "DROP TABLE set_quarantine"
