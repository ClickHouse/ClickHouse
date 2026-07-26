#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: the failpoint pauses every ReplicatedMergeTree mutation on the server
# Tag no-shared-merge-tree: the failpoint is in the ReplicatedMergeTree mutation task
# Tag no-replicated-database: additional replicas execute the same mutation

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

# `temporary_directories_lifetime = 1` makes the cleanup thread consider the temporary directory of
# an in-flight mutation old enough to be removed, which is exactly what the test needs to check.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt (num UInt32, num2 UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt/', '1')
    ORDER BY num
    SETTINGS min_bytes_for_wide_part = 0,
             temporary_directories_lifetime = 1,
             cleanup_delay_period = 1,
             cleanup_delay_period_random_add = 0,
             max_cleanup_delay_period = 1;

    INSERT INTO rmt SELECT number, number + 1 FROM numbers(1000);
"

# Pause the mutation right before its temporary part directory is renamed to the persistent name.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_before_rename_part;
    ALTER TABLE rmt RENAME COLUMN num2 TO foo2 SETTINGS alter_sync = 0;
"

wait_for_mutation_in_progress "rmt" "0000000000"

# The cleanup thread must skip the directory while the mutation still owns it. Wait until it looked
# at the directory at least once, then report what it decided to do with it.
CLEANUP_DECISION="message LIKE '%is in use (by merge/mutation/INSERT)%' OR message LIKE '%Removing temporary directory%'"
CLEANUP_ROWS="FROM system.text_log
    WHERE logger_name LIKE '${CLICKHOUSE_DATABASE}.rmt%' AND message LIKE '%tmp_mut_%' AND ($CLEANUP_DECISION)"

for _ in {1..100}
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() $CLEANUP_ROWS") -gt 0 ]]
    then
        break
    fi
    sleep 0.3
done

$CLICKHOUSE_CLIENT --query "
    SELECT
        countIf(message LIKE '%is in use (by merge/mutation/INSERT)%') > 0 AS kept,
        countIf(message LIKE '%Removing temporary directory%') AS removed
    $CLEANUP_ROWS;

    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_before_rename_part;
"

wait_for_mutation "rmt" "0000000000"

# The mutated part must be complete: every file listed in its checksums has to exist on disk.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM rmt WHERE foo2 % 1000 > 0;
    CHECK TABLE rmt SETTINGS check_query_single_value_result = 1;
    DROP TABLE rmt SYNC;
"
