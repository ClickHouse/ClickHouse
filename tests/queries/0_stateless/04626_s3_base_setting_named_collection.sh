#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3 (minio)
# Tag no-replicated-database: named collections are server-global, not database-scoped

# Test the s3_base setting when the S3 table engine takes its relative URL from a named collection.
# https://github.com/ClickHouse/ClickHouse/issues/59617

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BUCKET_URL="http://localhost:11111/test"
# ${CLICKHOUSE_TEST_UNIQUE_NAME} embeds this file's own test name, so the object and the collection
# below cannot collide with 04626_s3_base_setting running at the same time.
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"

# Prepare a file in minio using an absolute URL.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'TSV', 'n UInt32, s String') SELECT number, concat('str_', toString(number)) FROM numbers(5) SETTINGS s3_truncate_on_insert = 1"

echo '--- S3 table engine from a named collection with a relative URL, resolved URL is materialized into the DDL'
NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
# Never drop the named collection here: an interrupted previous run can leave both the collection and
# the table behind, and dropping only the collection is exactly the broken state described at the end
# of the test. Reuse a leftover collection instead of recreating it - its contents are deterministic -
# and let the `DROP TABLE IF EXISTS` below remove a leftover table.
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION IF NOT EXISTS ${NC} AS url = '${FILE}', access_key_id = 'test', secret_access_key = 'testtest', format = 'TSV'"
${CLICKHOUSE_CLIENT} -q "SET s3_base = '${BUCKET_URL}/'; DROP TABLE IF EXISTS test_s3_base_nc; CREATE TABLE test_s3_base_nc (n UInt32, s String) ENGINE = S3(${NC});"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE test_s3_base_nc FORMAT TabSeparatedRaw" | grep -cF "url = '${BUCKET_URL}/${FILE}'"
# The table must survive DETACH/ATTACH in a session where s3_base is not set.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE test_s3_base_nc"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_s3_base_nc"
# Drop the named collection only after the table drop has succeeded. Under the stress test the
# server can be killed mid-test; the failed `DROP TABLE` is then skipped while the `DROP NAMED
# COLLECTION` succeeds against the restarted server, leaving a table whose `ATTACH` references a
# missing named collection, and the next server restart fails to load metadata.
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_s3_base_nc" && ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}"
