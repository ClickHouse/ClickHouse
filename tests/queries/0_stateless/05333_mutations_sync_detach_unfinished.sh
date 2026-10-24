#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# A mutation waited for with mutations_sync fails with UNFINISHED when the table is detached first, and runs after ATTACH.
# Red if the ALTER UPDATE returns success although its mutation has not run.
# Plain MergeTree only: the replicated wait is a different path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_mutation_wait_detach;
    CREATE TABLE t_mutation_wait_detach (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_mutation_wait_detach VALUES (1, 0);
    SYSTEM STOP MERGES t_mutation_wait_detach;
"

(
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_mutation_wait_detach UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 2" 2>&1 \
        | grep -o -m1 "UNFINISHED" || echo "the UPDATE did not fail with UNFINISHED"
) &

for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_wait_detach'" | grep -q '^1$' && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "DETACH TABLE t_mutation_wait_detach"
wait
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_mutation_wait_detach"

for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_wait_detach' AND NOT is_done" | grep -q '^0$' && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mutation_wait_detach"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mutation_wait_detach"
