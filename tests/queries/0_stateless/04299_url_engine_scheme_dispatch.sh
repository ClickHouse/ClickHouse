#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` table function and `URL` engine dispatch by scheme: `file://` is served by the File engine.

REL="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
ABS="${USER_FILES_PATH}/${REL}"
printf '1,Hello\n2,World\n' > "$ABS"

echo "--- url('file://<absolute>') with explicit format/structure ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://${ABS}', 'CSV', 'a UInt32, b String') ORDER BY a"

echo "--- url('file://<relative>') resolves under user_files ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://${REL}', 'CSV', 'a UInt32, b String') ORDER BY a"

echo "--- format auto-detected from the .csv extension ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM url('file://${REL}')"

echo "--- url_base routes a relative reference to the File engine ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('${REL}', 'CSV', 'a UInt32, b String') ORDER BY a SETTINGS url_base = 'file://${USER_FILES_PATH}/'"

echo "--- CREATE TABLE ... ENGINE = URL('file://...') reads via the File engine ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t (a UInt32, b String) ENGINE = URL('file://${ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"

echo "--- INSERT INTO url('file://...') round-trips through the File engine ---"
OUT="${CLICKHOUSE_TEST_UNIQUE_NAME}_out.csv"
${CLICKHOUSE_CLIENT} -q "INSERT INTO TABLE FUNCTION url('file://${OUT}', 'CSV', 'a UInt32, b String') VALUES (3, 'Three')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://${OUT}', 'CSV', 'a UInt32, b String') ORDER BY a"

echo "--- reading outside user_files via a relative file:// path is rejected ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://../../../../../../../etc/passwd', 'CSV', 'a String')" 2>&1 \
    | grep -qiE "ACCESS_DENIED|not inside|not allowed|Exception" && echo "rejected" || echo "NOT REJECTED"

rm -f "$ABS" "${USER_FILES_PATH}/${OUT}"
