#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises S3 disks, which are not compiled into the fast-test build.
#
# `external_id` is part of the STS assume-role triple (`role_arn`, `role_session_name`,
# `external_id`), so in a dynamic `disk(...)` definition it is a credential/auth field:
# a `from_env`/`from_zk` placeholder in it must mark the disk as relying on server-managed
# credentials and be rejected under the user-query S3 credential restriction, while a
# literal `external_id` keeps working.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="s3_external_id_hardening_${DB}"
DISK="s3_external_id_disk_${DB}"
ROLE_ARN="arn:aws:iam::123456789012:role/Test_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
}
trap cleanup EXIT
cleanup

# An indirect `external_id` resolves against the server's environment, so even with an
# explicit key pair the disk definition is not vouched for by the query itself and must
# be rejected (same as an indirect `role_arn` or `role_session_name`).
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_env', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_env/',
        access_key_id = 'clickhouse', secret_access_key = 'clickhouse',
        role_arn = '${ROLE_ARN}', external_id = 'from_env ${DB}_EXTERNAL_ID')
    -- { serverError ACCESS_DENIED }
"

# Control: a literal `external_id` is a value supplied by the query itself and stays allowed.
# Without a `role_arn` no STS assume-role happens, so the disk works against the local minio.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_literal', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_literal/',
        access_key_id = 'clickhouse', secret_access_key = 'clickhouse',
        external_id = 'literal_external_id')
"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${TABLE}"
