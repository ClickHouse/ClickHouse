#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that http_max_multipart_form_data_size is enforced on decompressed data
# Create a user with a small limit, then send compressed data that exceeds it when decompressed

# User names are server-global, so scope the name to the test database to avoid
# collisions when the test runs concurrently (e.g. in the flaky check).
USER_NAME="test_decompress_limit_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 100"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# Generate compressed native block (100 rows = several KB when decompressed)
# With 100 byte limit, decompressed data should exceed the limit
# When limit is hit, LimitReadBuffer returns EOF which causes format reader to throw CANNOT_READ_ALL_DATA
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'CANNOT_READ_ALL_DATA'

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
