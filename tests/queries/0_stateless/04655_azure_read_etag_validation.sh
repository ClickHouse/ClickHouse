#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires azureBlobStorage
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT azure_read_inject_etag_mismatch" 2>/dev/null
}
trap on_exit EXIT

connection="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
container="cont04655${CLICKHOUSE_DATABASE}"
blob="04655_etag.csv"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SELECT number FROM numbers(100) SETTINGS azure_truncate_on_insert = 1"

# Force every download to report a different ETag than the one captured at read setup, simulating a
# concurrent in-place overwrite. This exercises the whole path: the `s3_validate_etag_on_read` gate in
# `StorageObjectStorageSource`, the ETag carried into `ReadBufferFromAzureBlobStorage`, and the check
# performed on the download response.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT azure_read_inject_etag_mismatch"

# Validation on (default): the injected mismatch must surface as `S3_OBJECT_CHANGED_DURING_READ`.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# Validation off: the ETag is not propagated, so the same read succeeds.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 0"
