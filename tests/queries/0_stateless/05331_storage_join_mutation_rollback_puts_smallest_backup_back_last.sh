#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses server-wide failpoints that would break the mutations of persistent `Join`
# tables of concurrently running tests.

# The failed rollback of the mutation is logged as an error, which is expected here.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation of a persistent `Join` moves every committed backup aside before it commits by
# installing the consolidated backup under the smallest of their numbers. The next start treats the
# presence of that backup in the table directory as the commit. So when the rollback of a failed
# mutation fails midway, it must not have put the smallest backup back already: otherwise the next
# start would take the mutation as committed and drop the backups that are still aside.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_mutation_rollback;
    CREATE TABLE join_mutation_rollback (k UInt64, v String) ENGINE = Join(ALL, LEFT, k) SETTINGS persistent = 1;
    INSERT INTO join_mutation_rollback VALUES (1, 'one');
    INSERT INTO join_mutation_rollback VALUES (2, 'two');
    INSERT INTO join_mutation_rollback VALUES (3, 'three');
"

# Fails right before the commit, when all three backups are aside, and then fails the rollback after
# the first backup is put back.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_before_commit"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_mutate_fail_putting_backup_back"
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_rollback DELETE WHERE k = 2 SETTINGS mutations_sync = 2" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_before_commit"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_mutate_fail_putting_backup_back"

echo "rows after the mutation that did not commit:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_rollback ORDER BY k"

# Reattaching finishes the interrupted rollback and rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_rollback;
    ATTACH TABLE join_mutation_rollback;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_rollback ORDER BY k"

# A regular mutation still works afterwards.
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_rollback DELETE WHERE k = 2 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_rollback;
    ATTACH TABLE join_mutation_rollback;
"
echo "rows after another mutation and reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_rollback ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_mutation_rollback"
