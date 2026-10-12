#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# Step 1: a partitioned Iceberg table with data files in two partitions.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

INSERTS=$(for i in $(seq 0 9); do echo "INSERT INTO ${TABLE} VALUES (1, ${i});"; echo "INSERT INTO ${TABLE} VALUES (2, ${i});"; done)
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${INSERTS}"

# Data compaction only runs when the history contains a positional delete, so without one
# the OPTIMIZE below is a no-op that never reaches the manifest writer.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE v = 3"

# Step 2: make the current partition spec longer than the data files' 1-value partition tuples by
# appending a second field, as partition evolution on an externally written table leaves it.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
meta["partition-specs"][0]["fields"].append(
    {"field-id": 1002, "name": "b", "source-id": 2, "transform": "identity"})
json.dump(meta, open(path, "w"))
PY

# Step 3: drop the in-memory metadata so the edited (longer) spec is re-read.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

# Step 4: the rewritten manifest entries carry each file's own partition tuple, which is now
# shorter than the spec the manifest is written against.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

# The server is still alive and the table was left untouched.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
