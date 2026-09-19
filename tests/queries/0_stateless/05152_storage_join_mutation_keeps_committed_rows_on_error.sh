#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would break the mutations of persistent `Join`
# tables of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation of a persistent `Join` consolidates the committed backups into a single file. It
# installs that file first, with one atomic replace of the lowest-numbered committed backup, and
# retires the superseded backups only afterwards. Removing them first would let a failure in the
# middle of the rewrite destroy rows that were committed before the mutation, and that truncated
# directory is what a restart would restore. Check that a failure between the two steps keeps every
# committed row -- it may only leave rows the mutation was supposed to delete.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_mutation_failure;
    CREATE TABLE join_mutation_failure (k UInt64, v String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1;
    INSERT INTO join_mutation_failure VALUES (1, 'one'), (2, 'two');
    INSERT INTO join_mutation_failure VALUES (3, 'three');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_after_promoting_consolidated_backup"

$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_failure DELETE WHERE k = 3 SETTINGS mutations_sync = 2" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_after_promoting_consolidated_backup"

echo "rows after the failed mutation:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_failure ORDER BY k"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_failure;
    ATTACH TABLE join_mutation_failure;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_failure ORDER BY k"

# Repeating the mutation without the injected failure completes the rewrite.
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_failure DELETE WHERE k = 3 SETTINGS mutations_sync = 2"
echo "rows after the repeated mutation:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_failure ORDER BY k"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_failure;
    ATTACH TABLE join_mutation_failure;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_failure ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_mutation_failure"
