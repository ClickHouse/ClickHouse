#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_manual_timeout (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual';
    INSERT INTO t_manual_timeout VALUES (1);
    INSERT INTO t_manual_timeout VALUES (2);
    SYSTEM STOP MERGES t_manual_timeout;
    SYSTEM SCHEDULE MERGE t_manual_timeout PARTS 'all_1_1_0', 'all_2_2_0';
"

# Merges are stopped, so the scheduled merge never happens and the command has to give up on its
# own elapsed-time check. Matching the message, not just code 159, keeps a timeout raised by the
# query machinery from passing for it.
$CLICKHOUSE_CLIENT --max_execution_time 1 -q "SYSTEM SYNC MERGES t_manual_timeout" 2>&1 \
    | grep -om1 "SYNC MERGES .*: command timed out\. See the 'max_execution_time' setting\. (TIMEOUT_EXCEEDED)" \
    | sed -E "s/SYNC MERGES [^:]*:/SYNC MERGES <table>:/"

$CLICKHOUSE_CLIENT -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_manual_timeout' AND active ORDER BY name;
"
