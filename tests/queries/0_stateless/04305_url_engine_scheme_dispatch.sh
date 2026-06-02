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

echo "--- ENGINE = URL('file://...') keeps its original syntax (reload-safe across DETACH/ATTACH) ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2 (a UInt32, b String) ENGINE = URL('file://${ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_t2'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2 ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t2"

echo "--- ENGINE = URL('file://...') without explicit columns persists the inferred structure ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3 ENGINE = URL('file://${ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = '${CLICKHOUSE_TEST_UNIQUE_NAME}_t3'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t3"

echo "--- headers(...) are rejected when dispatching to a non-URL scheme ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://${REL}', 'CSV', 'a UInt32, b String', headers('X-Test'='1'))" 2>&1 \
    | grep -qiE "does not support headers" && echo "headers-rejected" || echo "NOT REJECTED"

echo "--- reading outside user_files via a relative file:// path is rejected ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('file://../../../../../../../etc/passwd', 'CSV', 'a String')" 2>&1 \
    | grep -qiE "ACCESS_DENIED|not inside|not allowed|Exception" && echo "rejected" || echo "NOT REJECTED"

echo "--- url_base-resolved relative URL is persisted as an absolute file:// URL (reload-safe) ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4"
# `url_base` is applied as a query setting only at CREATE time (not on the later ATTACH), so the
# resolved absolute URL must be materialized into the persisted args to survive the reload.
${CLICKHOUSE_CLIENT} --url_base="file://${USER_FILES_PATH}/" -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4 (a UInt32, b String) ENGINE = URL('${REL}', 'CSV')"
# The persisted engine definition carries a file:// scheme, not the original relative reference.
${CLICKHOUSE_CLIENT} -q "SELECT engine_full LIKE '%file://%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_t4'"
# Reload without url_base still dispatches to the File engine via the persisted absolute URL.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4 ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t4"

echo "--- RENAME on ENGINE = URL('file://...') is metadata-only (URL semantics preserved) ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_r1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_r2"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_r1 (a UInt32, b String) ENGINE = URL('file://${ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_r1 TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_r2"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_r2 ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_r2"

echo "--- TRUNCATE on ENGINE = URL('file://...') is rejected (URL semantics preserved) ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_tr"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_tr (a UInt32, b String) ENGINE = URL('file://${ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_tr" 2>&1 \
    | grep -qiE "not supported|NOT_IMPLEMENTED" && echo "truncate-rejected" || echo "NOT REJECTED"
# The underlying file is left intact.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_tr"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_tr"

echo "--- urlCluster rejects non-URL schemes (no silent initiator-only read) ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM urlCluster('test_shard_localhost', 'file://${ABS}', 'CSV', 'a UInt32, b String')" 2>&1 \
    | grep -qiE "does not support the .* scheme|use the .*Cluster" && echo "cluster-rejected" || echo "NOT REJECTED"

rm -f "$ABS" "${USER_FILES_PATH}/${OUT}"
