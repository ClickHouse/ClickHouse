#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_copy_inject_etag_mismatch" 2>/dev/null
}
trap on_exit EXIT

# A table on an S3 disk backed up to S3: every file of it goes through the S3-to-S3 copy of the
# backup writer, which names the generation of the source object with one `HeadObject` and pins the
# copy to it with `x-amz-copy-source-if-match`.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE source (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS disk = 's3_disk'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO source SELECT number FROM numbers(100)"

# The failpoint makes the pinned copy carry a generation the source never had, which is what a source
# object replaced in place between the `HeadObject` and the copy looks like to the endpoint: the copy
# must be refused rather than taken from whatever is at the key by then.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT s3_copy_inject_etag_mismatch"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE source TO S3(s3_conn, 'backups/${CLICKHOUSE_DATABASE}/05211_replaced') SETTINGS allow_s3_native_copy = true" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# `s3_validate_etag_on_read = 0` opts the copies out of the pinning, as it does every other S3 read:
# they carry no generation, so the failpoint has nothing to replace and the backup goes through.
${CLICKHOUSE_CLIENT} --s3_validate_etag_on_read 0 -q "BACKUP TABLE source TO S3(s3_conn, 'backups/${CLICKHOUSE_DATABASE}/05211_unpinned') SETTINGS allow_s3_native_copy = true" > /dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_copy_inject_etag_mismatch"

# The same backup of a table nobody touched goes through pinned: the pinning refuses a replaced
# object, not every object.
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE source TO S3(s3_conn, 'backups/${CLICKHOUSE_DATABASE}/05211_unchanged') SETTINGS allow_s3_native_copy = true" > /dev/null
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS restored FROM S3(s3_conn, 'backups/${CLICKHOUSE_DATABASE}/05211_unchanged')" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM restored"
