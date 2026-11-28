#!/usr/bin/env bash
# Tags: no-fasttest
# A garbage or oversized iceberg_metadata_file_path (e.g. '.*', issue #109612)
# used to throw std::length_error / std::out_of_range from the metadata file
# name parser. Such a name is now read like any other, so the query must fail
# cleanly on the missing file.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The exact query from the fuzzer report.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '.*')
" 2>&1 | grep -o "S3_ERROR" | head -1

# issue #109612: oversized version, 'vN.metadata.json' form.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = 'v99999999999999999999.metadata.json')
" 2>&1 | grep -o "S3_ERROR" | head -1

# issue #109612: oversized version, 'N-<uuid>.metadata.json' form.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '99999999999999999999-abc.metadata.json')
" 2>&1 | grep -o "S3_ERROR" | head -1
