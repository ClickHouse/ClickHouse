#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_head_omit_etag" 2>/dev/null
}
trap on_exit EXIT

backup="S3('http://localhost:11111/test/05213_${CLICKHOUSE_DATABASE}.tar.gz', 'test', 'testtest')"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE source (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO source SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE source TO $backup" > /dev/null

# An archive is reopened for every handle the archive reader needs, and the whole session is pinned
# to the generation of the archive named when the backup is opened. The failpoint makes the endpoint
# report no ETag for the archive, so that generation cannot be named: the restore must be refused
# up front rather than opened as a session that nothing pins, whether or not plain reads are
# validated by `s3_validate_etag_on_read`.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT s3_head_omit_etag"
for validate in 1 0
do
    ${CLICKHOUSE_CLIENT} --s3_validate_etag_on_read ${validate} -q "RESTORE TABLE source AS unpinned FROM $backup" 2>&1 \
        | grep -oF "S3_ERROR" | head -n 1
done
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_head_omit_etag"

# The same restore against an endpoint that names the generation goes through.
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS restored FROM $backup" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM restored"
