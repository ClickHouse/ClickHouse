#!/usr/bin/env bash
# Tags: no-fasttest
# An Iceberg table whose metadata `location` names a sibling prefix of the same bucket, not the
# directory the table is read from -- the shape copying a table without rewriting its metadata
# produces. `IcebergPathResolver` must re-root the paths spelled relative to that location.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="05182_iceberg_same_bucket/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t_iceberg_same_bucket;
    CREATE TABLE t_iceberg_same_bucket (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
    INSERT INTO t_iceberg_same_bucket VALUES (42);
    DROP TABLE IF EXISTS t_iceberg_same_bucket;
"

# Only the last path component changes, so the bucket stays the configured one: this is a
# same-bucket mismatch, not a cross-bucket move.
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing 0 --output_format_parallel_formatting 0 -q "
    SELECT * FROM s3(s3_conn, filename='${TABLE_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
" | python3 -c "
import json, sys
m = json.load(sys.stdin)
old_location = m['location'].rstrip('/')
head, _, leaf = old_location.rpartition('/')
new_location = head + '/relocated_' + leaf
m['location'] = new_location
for s in m.get('snapshots', []):
    ml = s['manifest-list']
    for prefix in [old_location + '/', old_location]:
        if ml.startswith(prefix):
            s['manifest-list'] = new_location + '/' + ml[len(prefix):].lstrip('/')
            break
print(json.dumps(m))
" | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
    SETTINGS s3_truncate_on_insert=1
    SELECT * FROM input('line String') FORMAT LineAsString
"

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 -q "
    SELECT * FROM icebergS3(s3_conn, filename='${TABLE_PATH}');
"
