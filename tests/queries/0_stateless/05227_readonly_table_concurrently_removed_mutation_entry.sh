#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-parallel: uses a server-wide failpoint that pauses the loading of mutations of every MergeTree table
# Tag no-shared-merge-tree: does not support replication
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A readonly table over the directory of a live table (`table_disk` on the same endpoint) does not own the mutation
# entries there: the live table removes one at any moment, also between the listing and the read while the readonly
# table loads. The load fails with the reason instead of a missing object, and the next attempt succeeds.

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
disk_args="type = s3_plain_rewritable, endpoint = '${endpoint}', access_key_id = clickhouse, secret_access_key = clickhouse, enable_hard_links = 1"
failpoint="storage_merge_tree_load_mutations_pause_before_read"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}" 2>/dev/null
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS writer SYNC;
DROP TABLE IF EXISTS reader SYNC;

CREATE TABLE writer (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_writer', ${disk_args});

INSERT INTO writer SELECT number, toString(number) FROM numbers(100);

SYSTEM STOP MERGES writer;
ALTER TABLE writer UPDATE value = concat(value, '!') WHERE 1 SETTINGS mutations_sync = 0;
"

echo '-- the mutation of the live table is pending'
${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 'writer'"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

reader_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_reader.log"
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(readonly = true, name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_reader', ${disk_args})
" > "${reader_log}" 2>&1 &
reader_pid=$!

# The readonly table has listed the entry and is about to read it.
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

# The live table removes the entry meanwhile.
${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = 'writer' SYNC FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"

wait ${reader_pid} && echo 'the readonly table attached without the entry' || true

echo '-- the readonly table reports the concurrently removed entry, not a missing object'
grep -q 'was removed by the table that owns the directory while this table was loading it' "${reader_log}" && echo 'the reason is reported'
grep -q 'FILE_DOESNT_EXIST' "${reader_log}" && echo 'FILE_DOESNT_EXIST is reported'
grep -q 'S3_ERROR' "${reader_log}" && echo 'S3_ERROR is reported' || echo 'S3_ERROR is not reported'

echo '-- the next attempt succeeds once the metadata of the disk, which still lists the entry, is reloaded'
${CLICKHOUSE_CLIENT} -m --query "
SYSTEM DROP DISK METADATA CACHE ${CLICKHOUSE_TEST_UNIQUE_NAME}_reader;
CREATE TABLE reader (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(readonly = true, name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_reader', ${disk_args});
SELECT count() FROM reader;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'reader';

DROP TABLE reader SYNC;
SYSTEM START MERGES writer;
DROP TABLE writer SYNC;
"
