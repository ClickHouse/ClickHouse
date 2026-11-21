#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# A rollback makes an older snapshot current again and leaves the abandoned snapshots retained in
# `snapshots`. Compaction used to fold every retained snapshot into the rewrite - `getPlan` collects
# their files and `writeMetadataFiles` replays them in order - so a plain `OPTIMIZE` republished the
# abandoned branch and silently moved the table off the snapshot it was rolled back to.
#
# The row COUNT cannot tell the two states apart here (three rows either way), so the values are what
# is asserted: the rollbacked state is `1,2,3` and the abandoned branch tip is `1,3,4`. Those
# assertions hold on both builds - a cloud build gates `OPTIMIZE` on
# `IcebergCompactionMetadataGenerator` and throws instead of compacting, which also leaves the table
# alone. Only the open-source build is held to the refusal itself, since only there is it observable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup() {
    # An early exit between DETACH and ATTACH leaves the table detached, where DROP cannot see it.
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

# Roll the table back to the first append, the way a rollback does: `current-snapshot-id` and the
# branch refs point at the older snapshot, the later snapshots stay retained, and `snapshot-log`
# records that the older one became current again.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
ids = [snapshot["snapshot-id"] for snapshot in meta["snapshots"]]
assert len(ids) == 3, "expected three snapshots to roll back over, got %d" % len(ids)
target = ids[0]
meta["current-snapshot-id"] = target
for ref in (meta.get("refs") or {}).values():
    ref["snapshot-id"] = target
log = meta.setdefault("snapshot-log", [])
last_ms = max([entry["timestamp-ms"] for entry in log] + [meta.get("last-updated-ms", 0)])
log.append({"timestamp-ms": last_ms + 1, "snapshot-id": target})
meta["last-updated-ms"] = last_ms + 1
json.dump(meta, open(path, "w"))
PY

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

values() { ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --query "SELECT arrayStringConcat(arraySort(groupArray(v)), ',') FROM ${TABLE}"; }

echo "after rollback: $(values)"

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
ERR=$(${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_experimental_iceberg_compaction=1 \
    --send_logs_level=fatal --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if printf '%s' "${ERR}" | grep -qF 'ancestors of the current snapshot'; then
    echo "OPTIMIZE: refused"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    # Cloud gates the command on `IcebergCompactionMetadataGenerator`, so the refusal is not
    # reachable there; the row assertions are what this test relies on for that build.
    echo "OPTIMIZE: refused"
else
    echo "OPTIMIZE: FAIL - expected a refusal on the open-source build, got: ${ERR}"
fi

echo "after OPTIMIZE: $(values)"
