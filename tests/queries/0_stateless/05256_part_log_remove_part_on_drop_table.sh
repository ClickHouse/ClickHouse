#!/usr/bin/env bash
# Tags: zookeeper

# DROP TABLE removes every part of the table in dropAllData(), which bypasses removePartsFinally().
# It must still write one RemovePart event per part, as merges, DROP PARTITION and TRUNCATE do.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_part_log_drop SYNC;
    DROP TABLE IF EXISTS t_part_log_drop_replicated SYNC;
"

# system.part_log outlives the tables and their names are fixed, so only rows written after this point
# count: a rerun under a fixed --database would otherwise also see the rows of the previous run.
# Microseconds since epoch, so the comparison does not depend on the (randomized) session timezone.
start_time=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_part_log_drop (x UInt64) ENGINE = MergeTree ORDER BY x;
    SYSTEM STOP MERGES t_part_log_drop;
    INSERT INTO t_part_log_drop VALUES (1);
    INSERT INTO t_part_log_drop VALUES (2);
    DROP TABLE t_part_log_drop SYNC;

    CREATE TABLE t_part_log_drop_replicated (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_part_log_drop_replicated', 'r1') ORDER BY x;
    SYSTEM STOP MERGES t_part_log_drop_replicated;
    INSERT INTO t_part_log_drop_replicated VALUES (1);
    INSERT INTO t_part_log_drop_replicated VALUES (2);
    DROP TABLE t_part_log_drop_replicated SYNC;

    SYSTEM FLUSH LOGS part_log;

    SELECT table, event_type, count()
    FROM system.part_log
    WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $start_time
        AND database = currentDatabase() AND table IN ('t_part_log_drop', 't_part_log_drop_replicated')
    GROUP BY table, event_type
    ORDER BY table, event_type;
"
