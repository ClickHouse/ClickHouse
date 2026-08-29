#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/117000
# The `total-*` snapshot summary metrics are optional in the Iceberg spec, so a table committed by
# another engine can carry a current snapshot without `total-records`. `OPTIMIZE TABLE ... MANIFEST`
# only rewrites manifests, so every total is unchanged and an absent one stays absent. It used to be
# refused with BAD_ARGUMENTS instead, leaving compaction unavailable on a table that reads correctly.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# `strip` removes `total-records` from the current snapshot's summary, `report` describes the snapshot
# written on top of it.
metadata() {
    python3 - "${TABLE_PATH}metadata" "$1" <<'PY'
import json, os, re, sys
directory, mode = sys.argv[1], sys.argv[2]

def order(name):
    version = re.match(r"v?0*(\d+)", name)
    return (json.load(open(os.path.join(directory, name))).get("last-updated-ms", 0),
            int(version.group(1)) if version else 0)

names = [name for name in os.listdir(directory) if name.endswith(".metadata.json")]
path = os.path.join(directory, max(names, key=order))
meta = json.load(open(path))
by_id = {snapshot["snapshot-id"]: snapshot for snapshot in meta["snapshots"]}
current = by_id[meta["current-snapshot-id"]]

if mode == "strip":
    assert "total-records" in current["summary"], "the fixture never had total-records to remove"
    del current["summary"]["total-records"]
    json.dump(meta, open(path, "w"))
else:
    parent = by_id[current["parent-snapshot-id"]]
    # Byte sizes are build dependent, so report whether a total was carried over, not its value.
    carried = " ".join(
        "%s=%s" % (field, "carried" if current["summary"].get(field) == parent["summary"].get(field) else "changed")
        for field in ("total-files-size", "total-data-files"))
    print("new snapshot: operation=%s total-records=%s %s"
          % (current["summary"]["operation"],
             "absent" if "total-records" not in current["summary"] else "present", carried))
PY
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

# Both rows share partition a=1, so the two INSERTs leave two data manifests in one partition group.
# A rewrite is skipped once the data manifests are already at most one per partition, so this is the
# smallest shape that reaches the manifest-only snapshot.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
    INSERT INTO ${TABLE} VALUES (1, 10);
    INSERT INTO ${TABLE} VALUES (1, 20);
"

metadata strip

# An attached table keeps the metadata version it already parsed, so reload it to pick up the edit.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT 'rows', count(), sum(v) FROM ${TABLE}"

# Before the fix this printed `Cannot derive Iceberg snapshot total 'total-records'`.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=1" 2>&1 \
    | grep -oF 'BAD_ARGUMENTS'

metadata report

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT 'rows', count(), sum(v) FROM ${TABLE}"

# A commit that does change the total still cannot derive it from an absent one, so it must refuse.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
    --query "INSERT INTO ${TABLE} VALUES (1, 30)" 2>&1 | grep -oF 'BAD_ARGUMENTS' | head -n1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
