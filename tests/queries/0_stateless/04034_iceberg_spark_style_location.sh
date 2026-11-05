#!/usr/bin/env bash
# Tags: no-fasttest
# Positive test: Spark-created Iceberg table where metadata location field uses
# a completely different scheme/bucket (s3a://spark-bucket/...) than ClickHouse's path.
# The manifest-list entries use full absolute paths with Spark's URI.
# IcebergPathResolver::resolve should strip table_location prefix and prepend table_root.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="04034_iceberg_spark/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t_iceberg_spark;
    CREATE TABLE t_iceberg_spark (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
    INSERT INTO t_iceberg_spark VALUES (42);
    DROP TABLE IF EXISTS t_iceberg_spark;
"

# Simulate Spark: change location to s3a://spark-bucket/warehouse/spark_table
# and rewrite manifest-list paths to use the same Spark prefix.
SPARK_LOCATION='s3a://spark-bucket/warehouse/db/spark_table'
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing 0 --output_format_parallel_formatting 0 -q "
    SELECT * FROM s3(s3_conn, filename='${TABLE_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
" | python3 -c "
import json, sys
m = json.load(sys.stdin)
old_location = m['location'].rstrip('/')
new_location = '${SPARK_LOCATION}'.rstrip('/')
m['location'] = new_location
for s in m.get('snapshots', []):
    ml = s['manifest-list']
    # Strip old_location prefix (with or without trailing slash) and rejoin with new_location
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

# The query should succeed: IcebergPathResolver::resolve strips the Spark
# table_location prefix from the manifest-list path and prepends table_root.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 -q "
    SELECT * FROM icebergS3(s3_conn, filename='${TABLE_PATH}');
"
