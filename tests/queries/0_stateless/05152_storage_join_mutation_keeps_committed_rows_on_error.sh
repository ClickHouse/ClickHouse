#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses server-wide failpoints that would break the mutations of persistent `Join`
# tables of concurrently running tests.

# The failed cleanup of the superseded backups is logged as an error, which is expected here.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation of a persistent `Join` consolidates the committed backups into a single file. It moves
# the pre-mutation backups aside first and installs the consolidated backup with one atomic replace
# only after that, so the table directory never holds a mix of both generations: with `Join(ALL, ...)`
# such a mix would restore surviving rows twice. A failure before the replace must keep exactly the
# pre-mutation rows, live and after a restart. A failure while removing the superseded backups after
# the replace must not affect the committed mutation.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_mutation_error;
    CREATE TABLE join_mutation_error (k UInt64, v String) ENGINE = Join(ALL, LEFT, k) SETTINGS persistent = 1;
    INSERT INTO join_mutation_error VALUES (1, 'one'), (2, 'two');
    INSERT INTO join_mutation_error VALUES (3, 'three');
    INSERT INTO join_mutation_error VALUES (4, 'four');
"

# Fails after the first backup is moved aside, before the consolidated backup is installed.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_after_moving_backup_aside"
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_error DELETE WHERE k = 3 SETTINGS mutations_sync = 2" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_after_moving_backup_aside"

echo "rows after the mutation that did not commit:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error ORDER BY k"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_error;
    ATTACH TABLE join_mutation_error;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error ORDER BY k"

# Fails while removing the superseded backups, after the mutation committed.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_removing_superseded_backups"
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_error DELETE WHERE k = 3 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_removing_superseded_backups"
echo "rows after the committed mutation:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error ORDER BY k"

# The superseded backups left behind must not be restored.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_error;
    ATTACH TABLE join_mutation_error;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error ORDER BY k"

# A regular mutation still works afterwards.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_mutation_error VALUES (5, 'five')"
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_error DELETE WHERE k = 1 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_error;
    ATTACH TABLE join_mutation_error;
"
echo "rows after another mutation and reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_mutation_error"

# The same failure on a table with a single committed backup: the consolidated backup would be
# installed under the number of that very backup, so it must not be overwritten before the commit.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_mutation_error_single;
    CREATE TABLE join_mutation_error_single (k UInt64, v String) ENGINE = Join(ALL, LEFT, k) SETTINGS persistent = 1;
    INSERT INTO join_mutation_error_single VALUES (1, 'one'), (1, 'uno'), (2, 'two');
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_after_moving_backup_aside"
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_error_single DELETE WHERE k = 2 SETTINGS mutations_sync = 2" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_after_moving_backup_aside"
echo "single backup, rows after the mutation that did not commit:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error_single ORDER BY k, v"
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_error_single;
    ATTACH TABLE join_mutation_error_single;
"
echo "single backup, rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_error_single ORDER BY k, v"
$CLICKHOUSE_CLIENT --query "DROP TABLE join_mutation_error_single"
