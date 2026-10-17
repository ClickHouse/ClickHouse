#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree, no-ordinary-database
# Tag no-fasttest: creates custom object-storage disks
# Tag no-parallel: pauses every table drop with a server-wide failpoint that the test waits on
# Tag no-replicated-database: every replica would run the CREATE and mount the same disk root
# Tag no-shared-merge-tree: the parts found under the root are not the part set SharedMergeTree reads
# Tag no-ordinary-database: an Ordinary database drops inline, so no drop reaches the failpoints the test waits on

# A dropped `table_disk` table owns the root of its disk until the drop has finished, so another table cannot take
# the root while the drop can still fail and be retried.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
failpoint="database_catalog_drop_finally_before_id_erase"
after_notify="database_catalog_drop_finally_after_notify"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${after_notify}" 2>/dev/null
}
trap cleanup EXIT

function table_def()
{
    echo "$1 (k Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS table_disk = true, disk = disk(
        name = '${CLICKHOUSE_TEST_UNIQUE_NAME}', type = object_storage, object_storage_type = local,
        metadata_type = plain_rewritable, path = '${disk_path}')"
}

${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def writer)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC" &
drop_pid=$!

# The data of the table is removed, the drop has not finished yet.
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"
# Another table's drop can reach the failpoint first: also wait until this DROP has taken `writer` out of the database.
for _ in {1..100}; do
    [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'writer'") == 0 ]] && break
    sleep 0.1
done

echo '-- the root is still owned by the dropped table'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def second)" 2>&1 | grep -o -m1 'It is dropped.*(BAD_ARGUMENTS)'

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
wait ${drop_pid}

echo '-- after the drop has finished another table can use the root'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def second)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM second"
${CLICKHOUSE_CLIENT} --query "DROP TABLE second SYNC"

echo '-- DROP ... SYNC returns after the dropped table has released the root'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def writer)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${after_notify}"
# Another table's drop can reach the failpoint first and hold the drop queue: do not wait for this DROP unboundedly.
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC" &
drop_pid=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${after_notify} PAUSE"
for _ in {1..100}; do
    kill -0 ${drop_pid} 2>/dev/null || break
    sleep 0.1
done
if kill -0 ${drop_pid} 2>/dev/null; then
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${after_notify}"
fi
wait ${drop_pid}
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def third)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM third"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${after_notify}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE third SYNC"
