#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: dispatches to the S3/Azure object-storage backends (not built in the fast-test image)
# and relies on the `table_engines_require_grant` access-control improvement enabled for the
# stateless test server.
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# in-storage access check these engine-denial assertions rely on is a no-op and they silently allow.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dispatch re-checks `TABLE ENGINE` on the *target* backend on CREATE: a user granted only
# `TABLE ENGINE ON URL` can create http(s) URL tables (served by the URL engine itself) but is denied
# `s3://`/`az://` tables, which dispatch to `S3`/`AzureBlobStorage` and require those engine grants.
# The denial naming the target engine proves the dispatched backend (not the plain URL engine) is the
# one created. The http case is asserted here too, so a denial cannot pass merely because the user
# holds no engine grant at all.

S3_URL="s3://my-bucket/my-key.csv"

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

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
