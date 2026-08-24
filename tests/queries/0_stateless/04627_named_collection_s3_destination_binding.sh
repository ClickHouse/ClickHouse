#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OWN="http://localhost:11111/test"
OTHER="http://127.0.0.1:11112/test"

c() { echo "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"; }

# Only the refusal is asserted, never a successful S3 round trip: an allowed override is reported as
# "allowed" as soon as it gets past the check, whatever the request that follows does.
run() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 \
        | grep -qF "Override not allowed for 'url'" && echo refused || echo allowed
}

echo '--- credentialed collection, url moved to another origin'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c keys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c keys) AS
    url = '$OWN/', access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- same origin, different path: still allowed'
run "SELECT * FROM s3($(c keys), url = '$OWN/x.csv', format = 'CSV', structure = 'a String')"

echo '--- credential-free collection keeps full override freedom'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c anon)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c anon) AS url = '$OWN/'"
run "SELECT * FROM s3($(c anon), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- explicit OVERRIDABLE wins over the credential binding'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c open)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c open) AS
    url = '$OWN/' OVERRIDABLE, access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c open), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- query replaces the whole key pair: the collection supplies nothing, so no binding'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    access_key_id = 'other', secret_access_key = 'othersecret', format = 'CSV', structure = 'a String')"

echo '--- partial replacement: the stored secret_access_key still signs'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    access_key_id = 'other', format = 'CSV', structure = 'a String')"

echo '--- query-supplied role_arn: the collection keys are dropped, the query role authenticates'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    role_arn = 'arn:aws:iam::111111111111:role/r', format = 'CSV', structure = 'a String')"

echo '--- gcp_oauth sends a bearer token, so its ADC secrets bind the destination too'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcp)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcp) AS
    url = '$OWN/', http_client = 'gcp_oauth',
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- no_sign_request disables SigV4 only, the bearer token still goes out'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcpnosign)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcpnosign) AS
    url = '$OWN/', http_client = 'gcp_oauth', no_sign_request = 1,
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcpnosign), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- partial ADC replacement keeps the binding'
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv',
    google_adc_client_id = 'other', format = 'CSV', structure = 'a String')"

echo '--- backups: BackupInfo does not go through findOverrideForbiddingKey, so the seam is its own'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t; CREATE TABLE ${CLICKHOUSE_DATABASE}.t (a UInt8) ENGINE = Memory"
run "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO S3($(c keys), url = '$OTHER/bk')"

echo '--- DatabaseS3: getTableImpl rebuilds positional s3() args, so provenance is gone downstream'
run "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db ENGINE = S3($(c keys), url = '$OTHER/')"

echo '--- a relative stored url declares no origin, so a materialized s3_base replay still attaches'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c rel)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c rel) AS
    url = '${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION
    s3('$OWN/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', 'test', 'testtest', 'CSV', 'a UInt8') SELECT 1"
${CLICKHOUSE_CLIENT} -q "SET s3_base = '$OWN/';
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.replay (a UInt8) ENGINE = S3($(c rel))"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.replay"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t"
for n in keys anon open gcp gcpnosign rel; do
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c $n)"
done
