#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# Verify that a dynamic GCS disk created from user SQL cannot use `service_account_key_file` to make the
# server read an arbitrary local file path supplied by the query: `getDiskConfigurationFromAST.cpp` must
# reject it outright (this check does not depend on `USE_GOOGLE_CLOUD`, since it runs on the AST before the
# disk is handed to the object storage factory). An inline `service_account_key` (not a path) must still be
# allowed by this check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="gcs_key_file_${DB}"
DISK="gcs_key_file_disk_${DB}"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_a', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_a/',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_b', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_b/',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
"

# Positive control: an inline `service_account_key` (not a path) must not be rejected by this check. Table
# creation may still fail for an unrelated reason (no live GCS endpoint here, or the backend not compiled
# into this build), but that failure must not be `ACCESS_DENIED`.
out="$($CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_c', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_c/',
        service_account_key = '{}', skip_access_check = 1)" 2>&1)"
if echo "${out}" | grep -q "(ACCESS_DENIED)"; then
    echo "inline_key: fail (${out//$'\n'/ })"
else
    echo "inline_key: pass"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
