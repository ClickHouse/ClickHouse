#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `s3` table function is unavailable in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

check_bad_arguments()
{
    local query="$1"
    local error
    if error=$($CLICKHOUSE_CLIENT --query "$query" 2>&1); then
        echo "Query unexpectedly succeeded: $query" >&2
        exit 1
    fi
    if [[ "$error" != *"(BAD_ARGUMENTS)"* ]]; then
        echo "Expected BAD_ARGUMENTS for: $query" >&2
        echo "$error" >&2
        exit 1
    fi
}

check_access_denied()
{
    local query="$1"
    local error
    if error=$($CLICKHOUSE_CLIENT --user "$grant_user" --query "$query" 2>&1); then
        echo "Query unexpectedly succeeded without the required S3 grant: $query" >&2
        exit 1
    fi
    if [[ "$error" != *"(ACCESS_DENIED)"* ]]; then
        echo "Expected ACCESS_DENIED for: $query" >&2
        echo "$error" >&2
        exit 1
    fi
}

check_bad_arguments "SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', format='CSV', structure='x UInt8')"
check_bad_arguments "SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='', format='CSV', structure='x UInt8')"
check_bad_arguments "SELECT * FROM s3('arn:aws:s3:us-east-1:123456789012:accesspoint/example.mrap', key='key', format='CSV', structure='x UInt8')"
check_bad_arguments "SELECT * FROM s3('https://bucket.s3.amazonaws.com/key', key='another-key', format='CSV', structure='x UInt8')"

collection_name="mrap_invalid_${CLICKHOUSE_DATABASE}_${RANDOM}_${RANDOM}"
grant_user="mrap_grant_${CLICKHOUSE_DATABASE}_${RANDOM}_${RANDOM}"
cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${grant_user}"
    $CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS ${collection_name}"
}
trap cleanup EXIT
$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION ${collection_name} AS mrap_arn='arn:aws:s3::123456789012:accesspoint/example.mrap', key='', format='CSV', structure='x UInt8'"
check_bad_arguments "SELECT * FROM s3(${collection_name})"

$CLICKHOUSE_CLIENT --query "CREATE USER ${grant_user}"
$CLICKHOUSE_CLIENT --query "GRANT CREATE TEMPORARY TABLE ON *.* TO ${grant_user}"
read_query="SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='my file.csv', access_key_id='TEST', secret_access_key='TEST', format='CSV', structure='x UInt8') LIMIT 0 FORMAT Null"
check_access_denied "$read_query"
$CLICKHOUSE_CLIENT --query "GRANT READ ON S3('arn:aws:s3::123456789012:accesspoint/example[.]mrap/my%20file[.]csv') TO ${grant_user}"
check_access_denied "SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='other file.csv', access_key_id='TEST', secret_access_key='TEST', format='CSV', structure='x UInt8') LIMIT 0 FORMAT Null"

$CLICKHOUSE_CLIENT --query "GRANT READ ON S3('arn:aws:s3::123456789012:accesspoint/example[.]mrap/a/b') TO ${grant_user}"
check_access_denied "SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='a//b', access_key_id='TEST', secret_access_key='TEST', format='CSV', structure='x UInt8') LIMIT 0 FORMAT Null"

$CLICKHOUSE_CLIENT --query "GRANT READ ON S3('arn:aws:s3::123456789012:accesspoint/example[.]mrap/key') TO ${grant_user}"
check_access_denied "SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='/key', access_key_id='TEST', secret_access_key='TEST', format='CSV', structure='x UInt8') LIMIT 0 FORMAT Null"
