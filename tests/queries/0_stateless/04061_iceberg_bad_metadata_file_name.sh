#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: passing a garbage iceberg_metadata_file_path (e.g. '.*')
# used to cause std::length_error inside getMetadataFileAndVersion because
# find_first_of returned npos. After the fix it must return BAD_ARGUMENTS.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The exact query from the fuzzer report.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '.*')
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
