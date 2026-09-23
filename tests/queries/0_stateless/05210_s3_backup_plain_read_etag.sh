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

# A plain (non-archive) backup: every file of it is an object of its own, read through `readFile`.
backup="S3('http://localhost:11111/test/05210_${CLICKHOUSE_DATABASE}/', 'test', 'testtest')"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE source (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO source SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE source TO $backup" > /dev/null

# An ordinary read of a file of an unversioned backup is pinned to the generation one HEAD names
# for it. The failpoint makes every GET report another ETag, which is what an object replaced in
# place between the HEAD and the GET looks like: the restore must be refused rather than read as a
# splice of two generations.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT s3_read_inject_etag_mismatch"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS replaced FROM $backup" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# `s3_validate_etag_on_read = 0` opts the plain reads out of the pinning, as for every other S3 read.
${CLICKHOUSE_CLIENT} --s3_validate_etag_on_read 0 -q "RESTORE TABLE source AS unpinned FROM $backup" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM unpinned"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_read_inject_etag_mismatch"

# The same restore of a backup nobody touched goes through: the pinning refuses a replaced object,
# not every object.
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE source AS unchanged FROM $backup" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM unchanged"
