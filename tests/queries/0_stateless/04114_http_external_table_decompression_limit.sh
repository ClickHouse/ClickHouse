#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that http_max_multipart_form_data_size is enforced on decompressed data
# Create a user with a small limit, then send compressed data that exceeds it when decompressed

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_decompress_limit_user"
$CLICKHOUSE_CLIENT -q "CREATE USER test_decompress_limit_user IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 100"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO test_decompress_limit_user"

# Generate compressed native block (100 rows = several KB when decompressed)
# With 100 byte limit, decompressed data should exceed the limit
# When limit is hit, LimitReadBuffer returns EOF which causes format reader to throw CANNOT_READ_ALL_DATA
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=test_decompress_limit_user&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'CANNOT_READ_ALL_DATA'

$CLICKHOUSE_CLIENT -q "DROP USER test_decompress_limit_user"
