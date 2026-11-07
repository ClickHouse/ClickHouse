#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The unified `URL` engine dispatches recognized non-HTTP schemes to a delegate storage through the
# `StorageURLSchemeDispatch` wrapper, which forwards `getModificationHash` to the delegate. So a
# dispatched table's `system.tables.modification_hash` follows the delegate's contract, not the plain
# HTTP `URL` engine's ETag probe. For `file://` the delegate is `File`, which fails closed (a local
# file's size and mtime are weak validators), so the hash must be NULL - and reading the table must
# still work (the forwarding must not throw).

DATA_FILE="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
echo "1,foo" > "${DATA_FILE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f (a UInt32, b String) ENGINE = URL('file://${DATA_FILE}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT 'dispatched file url hash is null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_f'"
${CLICKHOUSE_CLIENT} -q "SELECT 'read works', * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"

rm -f "${DATA_FILE}"
