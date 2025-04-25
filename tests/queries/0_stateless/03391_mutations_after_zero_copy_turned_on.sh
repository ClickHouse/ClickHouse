#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations"
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations
    (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test03391_zero_copy_mutations', 'r1')
    ORDER BY key
    SETTINGS storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=0
"

$CLICKHOUSE_CLIENT -q "INSERT INTO $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations VALUES (1,1)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations MODIFY SETTING allow_remote_fs_zero_copy_replication=1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations UPDATE value=value WHERE 0"

for i in {1..10}
do
    if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='test03391_zero_copy_mutations' AND is_done=1")" -eq 1 ]; then
        break
    fi
    sleep 1
done

echo '1' | $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='test03391_zero_copy_mutations' AND is_done=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.test03391_zero_copy_mutations"
