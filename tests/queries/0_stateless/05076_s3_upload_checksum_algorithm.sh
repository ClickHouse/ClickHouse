#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3 (minio)
# Tag no-replicated-database: Named collection is used

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `upload_checksum_algorithm` is accepted as an `S3` named collection key, and an upload through
# that collection attaches the requested flexible checksum.
# Named collections live in a server-global namespace, so the name has to be unique per test run.
COLLECTION="collection_${CLICKHOUSE_TEST_UNIQUE_NAME}"
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${COLLECTION}"

${CLICKHOUSE_CLIENT} -q "
CREATE NAMED COLLECTION ${COLLECTION} AS
    url = 'http://localhost:11111/test/${FILE}',
    access_key_id = 'test',
    secret_access_key = 'testtest',
    format = 'CSV',
    structure = 'number UInt64',
    upload_checksum_algorithm = 'SHA256'"

${CLICKHOUSE_CLIENT} -q "INSERT INTO TABLE FUNCTION s3(${COLLECTION}) SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(number) FROM s3(${COLLECTION})"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${COLLECTION}"
