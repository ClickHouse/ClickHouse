#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-mergetree
# no-replicated-database: test relies on system.part_log but mutation can be executed on the second replica

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_delete_empty_part_rmt;

    CREATE TABLE t_delete_empty_part_rmt (a UInt64, b UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete_empty_part_rmt', '1')
    ORDER BY b PARTITION BY a;

    SET insert_keeper_fault_injection_probability = 0.0;

    INSERT INTO t_delete_empty_part_rmt SELECT 1, number FROM numbers(1000);
    INSERT INTO t_delete_empty_part_rmt SELECT 2, number FROM numbers(1000);
    INSERT INTO t_delete_empty_part_rmt SELECT 3, number FROM numbers(2000, 1000);

    SET mutations_sync = 2;
    ALTER TABLE t_delete_empty_part_rmt DELETE WHERE a = 2 OR b < 500;

    SELECT count() FROM t_delete_empty_part_rmt;
"

# Mutation query may return before the entry is added to part log.
# So, we may have to retry the flush of logs until all entries are actually flushed.
for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS part_log"
    res=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = 't_delete_empty_part_rmt' AND event_type = 'MutatePart'")

    if [[ $res -eq 3 ]]; then
        break
    fi

    sleep 2.0
done

${CLICKHOUSE_CLIENT} --query "
    SELECT
        part_name,
        ProfileEvents['MutationTotalParts'],
        ProfileEvents['MutationUntouchedParts'],
        ProfileEvents['MutationCreatedEmptyParts']
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_delete_empty_part_rmt' AND event_type = 'MutatePart'
    ORDER BY part_name;

    DROP TABLE t_delete_empty_part_rmt;
"
