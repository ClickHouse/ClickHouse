#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# `OPTIMIZE TABLE ... MANIFEST` walks the current manifest files (registering the `schema` copy from
# each manifest header) before it preloads the schemas of metadata.json. When a manifest header copy
# conflicts with metadata.json under the same schema-id, that later metadata.json registration has
# to follow `iceberg_tolerate_conflicting_manifest_schemas` of the current operation exactly like a
# read does: tolerant compaction replaces the header copy with the authoritative schema and
# succeeds, strict compaction fails with `ICEBERG_SPECIFICATION_VIOLATION`.
#
# The conflict is produced the way broken writers produce it: the manifest headers keep the
# `timestamp` type the data was written with, while metadata.json binds the same schema-id to
# `timestamptz`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The data is written as `timestamp` (parsed in the session time zone) and read back as `timestamptz`
# (rendered in UTC), so the session time zone has to be UTC for the values to round-trip unchanged.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --session_timezone UTC"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (ts DateTime64(6), v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# One INSERT per manifest, so the table has enough manifests to compact.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
    INSERT INTO ${TABLE} VALUES ('2024-01-01 00:00:00', 1);
    INSERT INTO ${TABLE} VALUES ('2024-01-02 00:00:00', 2);
    INSERT INTO ${TABLE} VALUES ('2024-01-03 00:00:00', 3);
"

# Rebind the schema-id in metadata.json to a schema that differs from the manifest header copies.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
for schema in meta["schemas"]:
    for field in schema["fields"]:
        if field["type"] == "timestamp":
            field["type"] = "timestamptz"
json.dump(meta, open(path, "w"))
PY

# Drop the in-memory metadata and the shared schema processor, so the compaction starts from an
# empty processor and the manifest header copies are registered before the metadata.json schemas.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

echo "strict compaction"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --iceberg_tolerate_conflicting_manifest_schemas=0 \
    --query "OPTIMIZE TABLE ${TABLE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=2" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo "tolerant compaction"
# The header copies are already registered by the strict attempt above; the tolerant run must still
# replace them with the metadata.json schema instead of inheriting the strictness of that attempt.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --iceberg_tolerate_conflicting_manifest_schemas=1 --send_logs_level=error \
    --query "OPTIMIZE TABLE ${TABLE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=2"

# The compaction rewrote three data manifests into one, and the data is intact.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --iceberg_tolerate_conflicting_manifest_schemas=1 --query "
    SELECT count() FROM ${TABLE};
    SELECT toTypeName(ts), ts, v FROM ${TABLE} ORDER BY v;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
