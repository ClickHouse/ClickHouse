#!/usr/bin/env bash
# Tags: no-fasttest
# The Spark-generated fixture retains snapshots written by Iceberg 0.11.1.
# See data_minio/iceberg_optional_snapshot_schema_id/README.md.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

HISTORICAL_SNAPSHOT_ID=$(python3 - "$CURDIR/data_minio/iceberg_optional_snapshot_schema_id/metadata/v4.metadata.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as metadata_file:
    metadata = json.load(metadata_file)
assert len(metadata["snapshots"]) == 2
assert all("schema-id" not in snapshot for snapshot in metadata["snapshots"])
print(metadata["snapshots"][0]["snapshot-id"])
PY
)

echo "Current snapshot without schema-id"
${CLICKHOUSE_CLIENT} --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id',
        SETTINGS iceberg_metadata_file_path='metadata/v4.metadata.json') ORDER BY c"

echo "Current snapshot with legacy history"
${CLICKHOUSE_CLIENT} --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id') ORDER BY c"

echo "Historical snapshot without schema-id"
${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${HISTORICAL_SNAPSHOT_ID}" --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id') ORDER BY c"

echo "Current snapshot with explicit null schema-id"
python3 - "$CURDIR/data_minio/iceberg_optional_snapshot_schema_id/metadata/v4.metadata.json" <<'PY' | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='iceberg_optional_snapshot_schema_id/metadata/v4_null_schema.metadata.json', structure='line String', format='LineAsString')
    SETTINGS s3_truncate_on_insert=1
    SELECT * FROM input('line String') FORMAT LineAsString
"
import json
import sys

with open(sys.argv[1]) as metadata_file:
    metadata = json.load(metadata_file)
assert len(metadata["snapshots"]) == 2
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = None
print(json.dumps(metadata))
PY

${CLICKHOUSE_CLIENT} --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id',
        SETTINGS iceberg_metadata_file_path='metadata/v4_null_schema.metadata.json') ORDER BY c"

echo "Historical snapshot with explicit null schema-id"
${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${HISTORICAL_SNAPSHOT_ID}" --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id',
        SETTINGS iceberg_metadata_file_path='metadata/v4_null_schema.metadata.json') ORDER BY c"

echo "Current snapshot from raw legacy v3 metadata"
${CLICKHOUSE_CLIENT} --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id',
        SETTINGS iceberg_metadata_file_path='metadata/v3.metadata.json') ORDER BY c"

echo "Historical snapshot from raw legacy v3 metadata"
${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${HISTORICAL_SNAPSHOT_ID}" --query "
    SELECT c FROM icebergS3(s3_conn, filename='iceberg_optional_snapshot_schema_id',
        SETTINGS iceberg_metadata_file_path='metadata/v3.metadata.json') ORDER BY c"


