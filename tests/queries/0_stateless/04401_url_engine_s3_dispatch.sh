#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: dispatches to the S3/Azure object-storage backends (not built in the fast-test image)
# and relies on the `table_engines_require_grant` access-control improvement enabled for the
# stateless test server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Positive object-storage dispatch checks for the unified `URL` engine / `url` table function:
# `s3://`/`gs`/`gcs`/`oss` route to the `S3` backend, `az://`/`azure`/`abfss` to `AzureBlobStorage`,
# while the `URL(...)` DDL syntax and `URL` engine semantics are preserved. These prove the dispatch
# actually reaches the object-storage delegate (rather than the plain `URL` engine) without doing any
# external I/O: the `URL`-engine syntax/reload checks are structural, and the dispatch is observed
# through the per-backend access control that fires before any read.

S3_URL="s3://my-bucket/my-key.csv"

echo "--- ENGINE = URL('s3://...') is reported as the URL engine but keeps the s3:// URL ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3 (a UInt32, b String) ENGINE = URL('${S3_URL}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3'"
${CLICKHOUSE_CLIENT} -q "SELECT engine_full LIKE '%${S3_URL}%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3'"

echo "--- the s3:// URL engine is reload-safe across DETACH/ATTACH (re-dispatches on reload) ---"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3"
${CLICKHOUSE_CLIENT} -q "SELECT engine_full LIKE '%${S3_URL}%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3'"

echo "--- TRUNCATE on ENGINE = URL('s3://...') is rejected (URL semantics preserved, no object removed) ---"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3" 2>&1 \
    | grep -qiE "not supported|NOT_IMPLEMENTED" && echo "truncate-rejected" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_s3"

# The dispatch re-checks `TABLE ENGINE` on the *target* backend on CREATE: a user granted only
# `TABLE ENGINE ON URL` can create http(s) URL tables (served by the URL engine itself) but is denied
# `s3://`/`az://` tables, which dispatch to `S3`/`AzureBlobStorage` and require those engine grants.
# The denial naming the target engine proves the dispatched backend (not the plain URL engine) is the
# one created.
USER="url_only_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON URL TO ${USER}"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('http://...') is allowed (URL engine) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http (a UInt32) ENGINE = URL('http://example.com/data.csv', 'CSV')" 2>&1 \
    | grep -qiE "Not enough privileges|ACCESS_DENIED" && echo "http-DENIED (unexpected)" || echo "http-allowed"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('s3://...') is denied (dispatches to S3) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_d_s3 (a UInt32) ENGINE = URL('${S3_URL}', 'CSV')" 2>&1 \
    | grep -qiE "TABLE ENGINE ON S3|grant.*\bS3\b" && echo "s3-engine-denied" || echo "NOT DENIED"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('az://...') is denied (dispatches to AzureBlobStorage) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_d_az (a UInt32) ENGINE = URL('az://account.blob.core.windows.net/container/blob.csv', 'CSV')" 2>&1 \
    | grep -qiE "TABLE ENGINE ON AzureBlobStorage|grant.*AzureBlobStorage" && echo "azure-engine-denied" || echo "NOT DENIED"

# The `url(...)` table function forwards the *delegate's* engine name to source-access control, so a
# user lacking the `S3` source grant is denied `url('s3://...')` with an `S3` (not `URL`) source
# error -- the denial fires before any read, and naming `S3` proves the table function dispatched to
# the `s3` backend.
echo "--- url('s3://...') table function requires the S3 source grant (dispatches to S3) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM url('${S3_URL}', 'CSV', 'a UInt32')" 2>&1 \
    | grep -qiE "grant.*\bS3\b|\bS3 ON\b" && echo "s3-source-required" || echo "NOT DENIED"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
