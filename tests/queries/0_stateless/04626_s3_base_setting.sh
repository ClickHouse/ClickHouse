#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

# Test the s3_base setting for resolving relative URLs in the s3 table function and the S3 table engine.
# https://github.com/ClickHouse/ClickHouse/issues/59617

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BUCKET_URL="http://localhost:11111/test"
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"

# Prepare a file in minio using an absolute URL.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SELECT number, concat('str_', toString(number)) FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"

echo '--- path-relative URL in the s3 table function'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM s3('${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') ORDER BY n SETTINGS s3_base = '${BUCKET_URL}/'"

echo '--- relative URL with a directory in the path'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('test/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SETTINGS s3_base = 'http://localhost:11111/'"

echo '--- dot segments are normalized'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('../${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SETTINGS s3_base = '${BUCKET_URL}/dir/'"

echo '--- absolute URL ignores s3_base'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SETTINGS s3_base = 'http://base.invalid/'"

echo '--- relative URL in an INSERT'
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SELECT number, concat('str_', toString(number)) FROM numbers(5) SETTINGS s3_base = '${BUCKET_URL}/', s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String')"

echo '--- s3Cluster with a relative URL'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3Cluster('test_shard_localhost', '${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SETTINGS s3_base = '${BUCKET_URL}/'"

echo '--- S3 table engine with a relative URL, resolved URL is materialized into the DDL'
${CLICKHOUSE_CLIENT} -q "SET s3_base = '${BUCKET_URL}/'; DROP TABLE IF EXISTS test_s3_base; CREATE TABLE test_s3_base (n UInt32, s String) ENGINE = S3('${FILE}', 'test', 'testtest', 'TSV');"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_s3_base"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE test_s3_base FORMAT TabSeparatedRaw" | grep -cF "S3('${BUCKET_URL}/${FILE}'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_s3_base"

echo '--- S3 table engine from a named collection with a relative URL, resolved URL is materialized into the DDL'
NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = '${FILE}', access_key_id = 'test', secret_access_key = 'testtest', format = 'TSV'"
${CLICKHOUSE_CLIENT} -q "SET s3_base = '${BUCKET_URL}/'; DROP TABLE IF EXISTS test_s3_base_nc; CREATE TABLE test_s3_base_nc (n UInt32, s String) ENGINE = S3(${NC});"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE test_s3_base_nc FORMAT TabSeparatedRaw" | grep -cF "url = '${BUCKET_URL}/${FILE}'"
# The table must survive DETACH/ATTACH in a session where s3_base is not set.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}"

echo '--- s3_base without a scheme is an error'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM s3('${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SETTINGS s3_base = 'localhost:11111/test/'" 2>&1 | grep -oF 'must contain a scheme' | head -1
