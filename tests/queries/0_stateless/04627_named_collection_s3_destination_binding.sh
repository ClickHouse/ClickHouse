#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OWN="http://localhost:11111/test"
OTHER="http://127.0.0.1:11112/test"

c() { echo "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"; }

DATA="${CLICKHOUSE_TEST_UNIQUE_NAME}_row.csv"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION
    s3('$OWN/$DATA', 'test', 'testtest', 'CSV', 'a String') SELECT 'payload'"

# For the refusal arms: the check either fires or it does not, and nothing downstream can produce this
# message.
run() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 \
        | grep -qF "Override not allowed for 'url'" && echo refused || echo allowed
}

# For the arms that must stay allowed. `run` reports "allowed" on any downstream failure, so a
# compatibility arm asserted that way cannot redden when the check becomes too broad. These assert the
# row instead: a real round trip to the collection's own origin.
allowed_reads() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 | grep -qxF payload && echo payload || echo "NOT-READ"
}

# For an arm that must pass the check but cannot complete: assert the *specific* downstream error, and
# assert the refusal is absent, so a refusal whose text happens to contain the pattern cannot pass.
allowed_fails_with() {
    local out
    out=$(${CLICKHOUSE_CLIENT} -m --query "$2" 2>&1)
    if grep -qF "Override not allowed for 'url'" <<< "$out"; then echo "REFUSED"
    elif grep -qF "$1" <<< "$out"; then echo "passed-check"
    else echo "NOT-REACHED"; fi
}

# For an arm whose credentials cannot read anything anywhere (an anonymous client against a bucket
# that requires auth): assert that a request was nevertheless issued. The check throws before any S3
# client is built, so an S3-level outcome of any kind proves it let the request through.
allowed_reaches_s3() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 | grep -qE 'payload|S3_ERROR' && echo "reached-s3" || echo "NOT-REACHED"
}

echo '--- credentialed collection, url moved to another origin'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c keys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c keys) AS
    url = '$OWN/', access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- same origin, different path: still allowed'
allowed_reads "SELECT * FROM s3($(c keys), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- credential-free collection keeps full override freedom'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c anon)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c anon) AS url = '$OTHER/'"
# A credential-free collection reads anonymously, which this bucket refuses, so assert the request was
# issued rather than a row returned.
allowed_reaches_s3 "SELECT * FROM s3($(c anon), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- explicit OVERRIDABLE wins over the credential binding'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c open)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c open) AS
    url = '$OTHER/' OVERRIDABLE, access_key_id = 'test', secret_access_key = 'testtest'"
allowed_reads "SELECT * FROM s3($(c open), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- query replaces the whole key pair: the collection supplies nothing, so no binding'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c otherkeys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c otherkeys) AS
    url = '$OTHER/', access_key_id = 'stored', secret_access_key = 'storedsecret'"
allowed_reads "SELECT * FROM s3($(c otherkeys), url = '$OWN/$DATA',
    access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV', structure = 'a String')"

echo '--- partial replacement: the stored secret_access_key still signs'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    access_key_id = 'other', format = 'CSV', structure = 'a String')"

echo '--- query-supplied role_arn: the collection keys are dropped, the query role authenticates'
# No STS endpoint answers here, so this arm cannot complete; assert the credential-resolution failure
# that is only reachable once the destination check has let the request through.
allowed_fails_with "role" "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
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

echo '--- under gcp_oauth the stored AWS keys are inert, so a complete ADC replacement releases'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcpkeys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcpkeys) AS
    url = '$OWN/', http_client = 'gcp_oauth',
    access_key_id = 'test', secret_access_key = 'testtest',
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcpkeys), url = '$OTHER/x.csv',
    google_adc_client_id = 'own', google_adc_client_secret = 'ownsecret',
    google_adc_refresh_token = 'owntoken', format = 'CSV', structure = 'a String')"

echo '--- no_sign_request with static keys: nothing signs, so the destination is not bound'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c nosign)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c nosign) AS
    url = '$OTHER/', access_key_id = 'test', secret_access_key = 'testtest', no_sign_request = 1"
allowed_reaches_s3 "SELECT * FROM s3($(c nosign), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- a collection that stores no url authorises no destination for its keys'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c keysonly)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c keysonly) AS
    access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c keysonly), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- filename cannot move the origin: an absolute value is rejected before any request'
# `path::operator/` replaces the left operand when the right is absolute, so pin the rejection: were
# `S3::URI` ever to accept such a value, the destination would move and this arm must be revisited.
for f in '//127.0.0.1:11112/test/x.csv' '/steal/x.csv'; do
    ${CLICKHOUSE_CLIENT} -m --query "SELECT * FROM s3($(c keys), filename = '$f',
        format = 'CSV', structure = 'a String')" 2>&1 \
        | grep -qF "Host is empty in S3 URI" && echo "no-host" || echo "REACHED-HOST"
done

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
for n in keys anon open otherkeys gcp gcpnosign gcpkeys nosign keysonly rel; do
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c $n)"
done
