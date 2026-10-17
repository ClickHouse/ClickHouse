#!/usr/bin/env bash
# Tags: no-replicated-database
# RESTORE raises the metadata version of a ReplicatedMergeTree table by re-setting its `metadata` znode once per version,
# several versions per ZooKeeper transaction. Each of them repeats the whole value, so with a large value the number of
# versions per transaction must be lower, to keep the transaction below the request size limit of ZooKeeper.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# A constraint of about 120 KB, which is stored in the `metadata` znode.
long_string=$(printf 'a%.0s' {1..120000})
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (id UInt64, CONSTRAINT c CHECK length('$long_string') > 0) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number FROM numbers(10)"

alters=""
for _ in {1..50}; do
    alters+="ALTER TABLE t ADD COLUMN tmp UInt8; ALTER TABLE t DROP COLUMN tmp; "
done
$CLICKHOUSE_CLIENT -q "$alters"
$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't'"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE t TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"

$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't'"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id) FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC"
