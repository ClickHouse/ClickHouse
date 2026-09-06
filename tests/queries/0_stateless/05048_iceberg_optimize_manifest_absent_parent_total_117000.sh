#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/117000
# The `total-*` snapshot summary metrics are optional in the Iceberg spec, so a table committed by
# another engine can carry a current snapshot without one of them. `OPTIMIZE TABLE ... MANIFEST`
# only rewrites manifests, so every total is unchanged and an absent one stays absent. It used to be
# refused with BAD_ARGUMENTS instead, leaving compaction unavailable on a table that reads correctly.
# The three data totals share one derivation, so each of them is stripped in turn here.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_117000_${RANDOM}"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
mkdir -p "${ROOT}"

# `strip` removes the named total from the current snapshot's summary, `report` describes the
# snapshot written on top of it.
metadata() {
    python3 - "$1" "$2" "$3" <<'PY'
import json, os, re, sys
directory, mode, stripped = sys.argv[1], sys.argv[2], sys.argv[3]

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
    assert stripped in current["summary"], "the fixture never had %s to remove" % stripped
    del current["summary"][stripped]
    json.dump(meta, open(path, "w"))
else:
    parent = by_id[current["parent-snapshot-id"]]

    # Byte sizes are build dependent, so report whether a total was carried over, not its value.
    def state(field):
        if field not in current["summary"]:
            return "absent"
        return "carried" if current["summary"][field] == parent["summary"].get(field) else "changed"

    print("new snapshot: operation=%s %s"
          % (current["summary"]["operation"],
             " ".join("%s=%s" % (field, state(field))
                      for field in ("total-records", "total-files-size", "total-data-files"))))
PY
}

absent_total_case() {
    local stripped=$1
    local table="t_${CLICKHOUSE_DATABASE}_${stripped//-/_}"
    local path="${ROOT}/${stripped}/"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (a Int32, v Int32)
        ENGINE = IcebergLocal('${path}', 'Parquet')
        PARTITION BY (a)
    "

    # Both rows share partition a=1, so the two INSERTs leave two data manifests in one partition
    # group. A rewrite is skipped once the data manifests are already at most one per partition, so
    # this is the smallest shape that reaches the manifest-only snapshot.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
        INSERT INTO ${table} VALUES (1, 10);
        INSERT INTO ${table} VALUES (1, 20);
    "

    metadata "${path}metadata" strip "${stripped}"

    # An attached table keeps the metadata version it already parsed, so reload it to pick up the edit.
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${table}"
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${table}"

    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT 'rows', count(), sum(v) FROM ${table}"

    # Before the fix this printed `Cannot derive Iceberg snapshot total '<stripped>'`.
    ${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
        --query "OPTIMIZE TABLE ${table} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=1" 2>&1 \
        | grep -oF 'BAD_ARGUMENTS'

    metadata "${path}metadata" report "${stripped}"

    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT 'rows', count(), sum(v) FROM ${table}"

    # A commit that does change the total still cannot derive it from an absent one, so it must refuse.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
        --query "INSERT INTO ${table} VALUES (1, 30)" 2>&1 | grep -oF 'BAD_ARGUMENTS' | head -n1

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table} SYNC"
}

for stripped in total-records total-files-size total-data-files; do
    echo "--- absent ${stripped}"
    absent_total_case "${stripped}"
done
