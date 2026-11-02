#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_read_inject_etag_mismatch" 2>/dev/null
}
trap on_exit EXIT

backup="S3('http://localhost:11111/test/05194_${CLICKHOUSE_DATABASE}.tar.gz', 'test', 'testtest')"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE source (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO source SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE source TO $backup" > /dev/null

# An archive is reopened for every handle the archive reader needs, so the session is pinned to the
# generation of the archive named when the backup was opened. The failpoint makes every GET report
# another ETag, which is what an archive replaced in place looks like: the restore must be refused
# rather than read as two archives.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT s3_read_inject_etag_mismatch"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS replaced FROM $backup" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_read_inject_etag_mismatch"

# The same restore of an archive nobody touched goes through: the pinning refuses a replaced
# archive, not every archive.
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS unchanged FROM $backup" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM unchanged"
