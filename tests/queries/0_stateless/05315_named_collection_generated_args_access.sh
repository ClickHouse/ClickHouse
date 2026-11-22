#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3 (minio)
# Tag no-replicated-database: named collections are server-global, not database-scoped

# ClickHouse appends inferred `format` and `structure` values to the arguments of named collection engines and table
# functions and parses them again. Replacing a stored `'auto'` value this way needs no secrets privilege.
# Resolving a relative stored `url` against `s3_base` or `url_base` replaces the stored `url`, so it needs the privilege.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

BUCKET_URL="http://localhost:11111/test"
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"
nc="${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="${nc}_user"
table="${CLICKHOUSE_DATABASE}.${nc}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS ${table}_url;
        DROP TABLE IF EXISTS ${table}_s3;
        DROP TABLE IF EXISTS ${table}_url_relative;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS ${nc}_url_auto;
        DROP NAMED COLLECTION IF EXISTS ${nc}_s3_auto;
        DROP NAMED COLLECTION IF EXISTS ${nc}_s3_relative;
        DROP NAMED COLLECTION IF EXISTS ${nc}_url_relative;
        DROP NAMED COLLECTION IF EXISTS ${nc}_locked;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SELECT number, concat('str_', toString(number)) FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE NAMED COLLECTION ${nc}_url_auto AS
        url = '${BUCKET_URL}/${FILE}', format = 'auto', structure = 'auto';
    CREATE NAMED COLLECTION ${nc}_s3_auto AS
        url = '${BUCKET_URL}/${FILE}', access_key_id = 'test', secret_access_key = 'testtest', format = 'auto', structure = 'auto';
    CREATE NAMED COLLECTION ${nc}_s3_relative AS
        url = '${FILE}', access_key_id = 'test', secret_access_key = 'testtest', format = 'TSV';
    CREATE NAMED COLLECTION ${nc}_url_relative AS
        url = '?query=SELECT+1', format = 'TSV';
    CREATE NAMED COLLECTION ${nc}_locked AS
        url = '${BUCKET_URL}/${FILE}', format = 'auto' NOT OVERRIDABLE;
    CREATE USER $user;
    GRANT S3, URL ON *.* TO $user;
    GRANT CREATE TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON * TO $user;
"

echo 'Inferred format and structure replace a stored auto value without the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE TABLE ${table}_url ENGINE = URL(${nc}_url_auto);
"
${CLICKHOUSE_CLIENT} --user "$user" --query "SHOW CREATE TABLE ${table}_url FORMAT TabSeparatedRaw" | grep -c "format = 'TSV'"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM ${table}_url"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', ${nc}_s3_auto)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM s3(${nc}_s3_auto)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM url(${nc}_url_auto)"

echo 'A stored auto value that is not overridable stays locked'
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM url(${nc}_locked, format = 'CSV')" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo 'Resolving a stored relative s3 url against s3_base needs the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --query "SET s3_base = '${BUCKET_URL}/'; CREATE TABLE ${table}_s3 (n UInt32, s String) ENGINE = S3(${nc}_s3_relative)" 2>&1 | grep -o 'ACCESS_DENIED' | head -1
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM s3(${nc}_s3_relative, structure = 'n UInt32, s String') SETTINGS s3_base = '${BUCKET_URL}/'" 2>&1 | grep -o 'ACCESS_DENIED' | head -1
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', ${nc}_s3_relative, structure = 'n UInt32, s String') SETTINGS s3_base = '${BUCKET_URL}/'" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo 'Resolving a stored relative url against url_base needs the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM url(${nc}_url_relative, structure = 'x UInt8') SETTINGS url_base = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/'" 2>&1 | grep -o 'ACCESS_DENIED' | head -1
${CLICKHOUSE_CLIENT} --user "$user" --query "SET url_base = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/'; CREATE TABLE ${table}_url_relative (x UInt8) ENGINE = URL(${nc}_url_relative)" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc}_s3_relative TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc}_url_relative TO $user"

echo 'With the privilege the relative urls are resolved'
${CLICKHOUSE_CLIENT} --user "$user" --query "SET s3_base = '${BUCKET_URL}/'; CREATE TABLE ${table}_s3 (n UInt32, s String) ENGINE = S3(${nc}_s3_relative)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM ${table}_s3"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM s3(${nc}_s3_relative, structure = 'n UInt32, s String') SETTINGS s3_base = '${BUCKET_URL}/'"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM url(${nc}_url_relative, structure = 'x UInt8') SETTINGS url_base = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/'"
${CLICKHOUSE_CLIENT} --user "$user" --query "SET url_base = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/'; CREATE TABLE ${table}_url_relative (x UInt8) ENGINE = URL(${nc}_url_relative)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM ${table}_url_relative"
