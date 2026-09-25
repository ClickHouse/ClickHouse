#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would break the rollback of persistent `Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent insert into a `Join` table that exceeds `max_rows_in_join` while its promoted backup
# is replayed into the live state is rolled back by rebuilding the state from the committed backups.
# That rebuild can throw as well (for example, on memory limits). The partially replayed state must
# not stay visible then: readers get an exception until the next operation under the write lock
# rebuilds the state from the backup files.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_rollback_error;
    CREATE TABLE join_rollback_error (k UInt64, v String) ENGINE = Join(ALL, LEFT, k)
        SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
    INSERT INTO join_rollback_error VALUES (1, 'committed');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "INSERT INTO join_rollback_error VALUES (2, 'over_limit'), (3, 'over_limit')" 2>&1 | grep -o 'SET_SIZE_LIMIT_EXCEEDED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_publish_fail_during_rollback"

echo "reads after the rollback could not restore the state:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_rollback_error ORDER BY k" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1
$CLICKHOUSE_CLIENT --query "SELECT count() FROM join_rollback_error" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1
$CLICKHOUSE_CLIENT --query "SELECT joinGet('join_rollback_error', 'v', toUInt64(1))" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1
$CLICKHOUSE_CLIENT --query "SELECT n, v FROM (SELECT toUInt64(1) AS n) AS l ANY LEFT JOIN join_rollback_error AS r ON l.n = r.k" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1

# The next insert rebuilds the state from the committed backups before publishing its own rows.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_rollback_error VALUES (4, 'next')"
echo "rows after the next insert:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_rollback_error ORDER BY k"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_rollback_error;
    ATTACH TABLE join_rollback_error;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_rollback_error ORDER BY k"

# `OPTIMIZE` rebuilds the lost state too.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "INSERT INTO join_rollback_error VALUES (5, 'over_limit')" 2>&1 | grep -o 'SET_SIZE_LIMIT_EXCEEDED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_rollback_error ORDER BY k" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE join_rollback_error"
echo "rows after optimize:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_rollback_error ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_rollback_error"
