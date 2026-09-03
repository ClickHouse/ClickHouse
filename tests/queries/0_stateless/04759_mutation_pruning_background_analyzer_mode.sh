#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation executes in the background with a context derived from the background context, so a
# session-only analyzer switch does not propagate to it. The partition pruning analysis must
# follow the analyzer mode of that background execution, not of the submitting session; otherwise
# an analyzer-only predicate (here: a qualified column name) submitted with the session analyzer
# mode opposite to the server one would pass the submit-time analysis and enqueue a mutation that
# only fails later in the background. With the pruning analysis pinned to the background context,
# the mismatch fails fast at submit time and nothing is enqueued.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_prune_bg_analyzer SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_prune_bg_analyzer (p UInt8, y UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_bg_analyzer', 'r1')
    PARTITION BY p ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_prune_bg_analyzer VALUES (1, 0), (2, 0)"

server_analyzer_mode=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.settings WHERE name = 'allow_experimental_analyzer'")
if [ "$server_analyzer_mode" = "1" ]; then opposite=0; else opposite=1; fi

$CLICKHOUSE_CLIENT --allow_experimental_analyzer="$opposite" --optimize_mutations_with_partition_pruning=1 \
    -q "ALTER TABLE t_mut_prune_bg_analyzer UPDATE y = 1 WHERE t_mut_prune_bg_analyzer.p = 1" 2>&1 \
    | grep -oF 'UNKNOWN_IDENTIFIER' | head -1

echo "mutations after failed alter"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_prune_bg_analyzer'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_prune_bg_analyzer SYNC"
