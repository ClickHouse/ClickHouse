#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/92348
# Iceberg std::out_of_range exception when metadata `location` field differs from
# ClickHouse table path (e.g. table created by Spark with a different URI scheme/path).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="04033_iceberg_mismatched/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t_iceberg_mismatch;
    CREATE TABLE t_iceberg_mismatch (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
    INSERT INTO t_iceberg_mismatch VALUES (1);
    DROP TABLE IF EXISTS t_iceberg_mismatch;
"

# Modify metadata: set location to a very long path, manifest-list to just the filename.
# This simulates a Spark-created table where ClickHouse rewrites the location field
# but Spark's manifest-list entries use short relative paths.
# Before the fix this caused std::out_of_range exception in IcebergPathResolver::resolve
# because table_location is longer than data_path (the manifest filename),
# causing unsigned underflow and out-of-range substr.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM s3(s3_conn, filename='${TABLE_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
" | python3 -c "
import json, sys
m = json.load(sys.stdin)
m['location'] = 's3://some-bucket/warehouse/very/deep/nested/path/to/a/spark/created/table/location/with/extra/long/segments/to/ensure/prefix/exceeds/data/path/length'
for s in m.get('snapshots', []):
    s['manifest-list'] = s['manifest-list'].rsplit('/', 1)[-1]
print(json.dumps(m))
" | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
    SETTINGS s3_truncate_on_insert=1
    SELECT * FROM input('line String') FORMAT LineAsString
"

# Disable iceberg metadata cache as a client-level setting so the modified metadata
# file is actually re-read (not served from cache populated during CREATE TABLE above).
# Before the fix this would crash with std::out_of_range in IcebergPathResolver::resolve.
# After the fix it returns a proper BAD_ARGUMENTS error about unresolvable path.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 -q "
    SELECT * FROM icebergS3(s3_conn, filename='${TABLE_PATH}');
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
