#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-parallel: uses a server-wide failpoint that pauses the drop of every MergeTree table
# Tag no-shared-merge-tree: does not support replication
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `table_disk` the table occupies the root of the disk, so `DROP TABLE` removes the mutation entries and the
# deduplication log by name. A drop that fails while removing the parts is retried, and `UNDROP TABLE` can restore the
# table until then, so that state has to stay until the parts are gone: a readonly table over the same directory still
# loads the mutation while the drop is paused before removing the parts.

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
disk_args="type = s3_plain_rewritable, endpoint = '${endpoint}', access_key_id = clickhouse, secret_access_key = clickhouse, enable_hard_links = 1"
failpoint="merge_tree_drop_all_data_pause_before_removing_parts"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}" 2>/dev/null
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS writer SYNC;
DROP TABLE IF EXISTS reader SYNC;

CREATE TABLE writer (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_writer', ${disk_args}),
         non_replicated_deduplication_window = 100;

INSERT INTO writer VALUES (1, 'a'), (2, 'b'), (3, 'c');
ALTER TABLE writer UPDATE value = concat(value, '!') WHERE 1 SETTINGS mutations_sync = 1;
SELECT count(), countIf(is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 'writer';
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC" &
drop_pid=$!

# The drop is about to remove the parts.
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

echo '-- a readonly table over the same directory still sees the parts, the mutation and the deduplication log'
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE reader (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(readonly = true, name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_reader', ${disk_args});
SELECT count(), countIf(endsWith(value, '!')) FROM reader;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'reader';
SELECT count() > 0 FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One') WHERE _path LIKE '%deduplication_log%';
"

# The failpoint pauses every drop, so the readonly table is dropped only after it is disabled.
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
wait ${drop_pid}
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"

echo '-- the completed drop leaves no objects behind'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One')"
