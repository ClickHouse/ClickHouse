#!/usr/bin/env bash
# Tags: no-fasttest
# A FileLog table whose stored definition has a path outside `user_files_path` still loads (it stays
# registered, but does not read anything), while a new definition with such a path is rejected,
# also by a full-definition ATTACH.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS filelog_bad_path_attach SYNC;"

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE filelog_bad_path_attach UUID '${UUID}' (k UInt8, v UInt8) ENGINE = FileLog('/tmp/nonexistent_4202_pr62421.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }"

# The stored definition of a table with a valid path is rewritten to the outside path, and the table
# is attached back with the short ATTACH. Error lines on stderr are expected.
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', 'CSV') SELECT 1 AS k, 1 AS v"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE filelog_bad_path_attach (k UInt8, v UInt8) ENGINE = FileLog('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', 'CSV');"
metadata_file=$(${CLICKHOUSE_CLIENT} --query "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach'")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE filelog_bad_path_attach;"
sed -i.bak "s|'${CLICKHOUSE_TEST_UNIQUE_NAME}.csv'|'/tmp/nonexistent_4202_pr62421.csv'|" "$metadata_file" && rm -f "$metadata_file.bak"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE filelog_bad_path_attach;" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';"
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'filelog_bad_path_attach';"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS filelog_bad_path_attach SYNC;"
rm -f "${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
