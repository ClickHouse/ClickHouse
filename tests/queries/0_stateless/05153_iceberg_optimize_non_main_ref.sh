#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Compaction rebuilds a table's metadata from `createEmptyMetadataFile`, and `MetadataGenerator`
# recreates only `refs.main`. A table that also keeps a named branch or tag therefore came back
# from a plain `OPTIMIZE` without it - metadata the user can see, and a snapshot that ref used to
# protect could afterwards be expired. The ref names an ancestor of the current snapshot, so the
# current-ancestor guard does not catch this shape.
#
# The assertions hold on both builds: a cloud build gates `OPTIMIZE` on
# `IcebergCompactionMetadataGenerator` and throws before reaching the check, which also leaves the
# refs alone. Only the open-source build is held to the refusal itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE IF NOT EXISTS ${TABLE}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC" 2>/dev/null
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 1), (1, 2), (1, 3)"
# A position delete is what makes compaction consider the table worth rewriting.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 4)"

latest_metadata() {
    ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1
}

# Tag an ancestor of the current snapshot, the way an external writer marks a version to keep.
python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
ancestor = meta["snapshots"][0]["snapshot-id"]
meta.setdefault("refs", {})["audit_tag"] = {"snapshot-id": ancestor, "type": "tag"}
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

refs() {
    python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
print(",".join(sorted(json.load(open(sys.argv[1])).get("refs", {}))))
PY
}
values() { ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --query "SELECT arrayStringConcat(arraySort(groupArray(v)), ',') FROM ${TABLE}"; }

echo "refs before: $(refs)"

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
    --send_logs_level=fatal --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if printf '%s' "${ERR}" | grep -qF 'only reference is'; then
    echo "OPTIMIZE: refused"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    # Cloud gates the command on `IcebergCompactionMetadataGenerator`, so the refusal is not
    # reachable there; the assertions around it are what this test relies on for that build.
    echo "OPTIMIZE: refused"
else
    echo "OPTIMIZE: FAIL - expected a refusal on the open-source build, got: ${ERR}"
fi

echo "refs after: $(refs)"
echo "rows: $(values)"

# `main` itself may carry a retention override, and the rewrite recreates it with only a snapshot id
# and a type, so those fields would go the same way a whole reference does.
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}"

TABLE="t_${CLICKHOUSE_DATABASE}_main_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 1), (1, 2), (1, 3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 4)"

python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
meta["refs"]["main"]["min-snapshots-to-keep"] = 5
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

main_retention() {
    python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["refs"]["main"].get("min-snapshots-to-keep", "<dropped>"))
PY
}

echo "main min-snapshots-to-keep before: $(main_retention)"
ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
    --send_logs_level=fatal --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if printf '%s' "${ERR}" | grep -qF 'Remove that retention override'; then
    echo "OPTIMIZE: refused"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    echo "OPTIMIZE: refused"
else
    echo "OPTIMIZE: FAIL - expected a refusal on the open-source build, got: ${ERR}"
fi

echo "main min-snapshots-to-keep after: $(main_retention)"

# The same retention floor can be set as a table property, and the rewrite carries no `properties`
# at all, so it would be lifted just as silently.
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}"

TABLE="t_${CLICKHOUSE_DATABASE}_prop_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 1), (1, 2), (1, 3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 4)"

python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
meta.setdefault("properties", {})["history.expire.min-snapshots-to-keep"] = "5"
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

table_retention() {
    python3 - "${TABLE_PATH}metadata/v$(latest_metadata).metadata.json" <<'PY'
import json, sys
props = json.load(open(sys.argv[1])).get("properties") or {}
print(props.get("history.expire.min-snapshots-to-keep", "<dropped>"))
PY
}

echo "table min-snapshots-to-keep before: $(table_retention)"
ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
    --send_logs_level=fatal --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if printf '%s' "${ERR}" | grep -qF 'does not preserve the table property'; then
    echo "OPTIMIZE: refused"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    echo "OPTIMIZE: refused"
else
    echo "OPTIMIZE: FAIL - expected a refusal on the open-source build, got: ${ERR}"
fi

echo "table min-snapshots-to-keep after: $(table_retention)"
