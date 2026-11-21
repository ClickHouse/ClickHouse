#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# Tag no-shared-merge-tree: the test hand-crafts a part node of a ReplicatedMergeTree replica.
# Tag no-replicated-database: the test uses an explicit replica name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A drop range whose working set is already empty must still remove the part nodes it covers.
# The node is in the range of the second drop only (all_0_1_*), not of the first one (all_0_0_*).

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05317_rmt"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rmt SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rmt (n int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05317_rmt', '1') ORDER BY n
    SETTINGS cleanup_delay_period = 1, max_cleanup_delay_period = 2, cleanup_delay_period_random_add = 0"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "INSERT INTO rmt VALUES (1)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rmt"

# The drop leaves an empty covering part that the next drop would remove, so wait until the table has
# no part in any state (system.parts omits Deleting parts unless _state is referenced).
for _ in {1..60}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND _state != ''")" = "0" ] && break
    sleep 1
done
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND _state != ''"

$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/replicas/1/parts', 'all_1_1_0', '')"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/replicas/1/parts' AND name = 'all_1_1_0'"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rmt"

for _ in {1..60}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/replicas/1/parts' AND name = 'all_1_1_0'")" = "0" ] && break
    sleep 1
done
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/replicas/1/parts' AND name = 'all_1_1_0'"

$CLICKHOUSE_CLIENT -q "DROP TABLE rmt SYNC"
