#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and python3 pyarrow.
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114854:
# Iceberg metadata declared `timestamp` while Parquet data files were written with
# isAdjustedToUTC=true. Spec-following readers reject that mismatch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_ts"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (k Int64, ts DateTime, ts64 DateTime64(6))
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE}
    SELECT
        number,
        toDateTime('2026-01-01 00:00:00') + number * 3600,
        toDateTime64('2026-01-01 00:00:00', 6) + number * 3600
    FROM numbers(5)
"

read -r -d '' PROBE <<'PY'
import glob, json, os, sys
import pyarrow.parquet as pq

metadata_dir = sys.argv[1]
data_glob = sys.argv[2]

metadata_files = sorted(glob.glob(os.path.join(metadata_dir, '*.metadata.json')))
if not metadata_files:
    raise SystemExit('no metadata file')
metadata = json.load(open(metadata_files[-1]))
schema = metadata['schemas'][-1]
types = {field['name']: field['type'] for field in schema['fields']}
print('metadata ts=%s' % types['ts'])
print('metadata ts64=%s' % types['ts64'])

parquet_files = sorted(glob.glob(data_glob, recursive=True))
if not parquet_files:
    raise SystemExit('no parquet file')
schema = pq.read_schema(parquet_files[0])
print('parquet ts=%s' % schema.field('ts').type)
print('parquet ts64=%s' % schema.field('ts64').type)
PY

python3 -c "$PROBE" "${TABLE_PATH}metadata" "${TABLE_PATH}data/**/*.parquet"

${CLICKHOUSE_CLIENT} --query "
    SELECT k, ts, ts64
    FROM ${TABLE}
    ORDER BY k
"
