#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: passing a garbage iceberg_metadata_file_path (e.g. '.*')
# used to cause std::length_error inside getMetadataFileAndVersion because
# find_first_of returned npos. After the fix it must return BAD_ARGUMENTS.
# Also covers issue #109612: an all-digit but oversized version number
# (> INT_MAX) used to throw std::out_of_range from std::stoi (STD_EXCEPTION)
# instead of BAD_ARGUMENTS. After the fix both forms return BAD_ARGUMENTS.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The exact query from the fuzzer report.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '.*')
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

# issue #109612: oversized version, 'vN.metadata.json' form.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = 'v99999999999999999999.metadata.json')
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

# issue #109612: oversized version, 'N-<uuid>.metadata.json' form.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '99999999999999999999-abc.metadata.json')
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
