#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-replicated-database: there the ALTER runs as replicated DDL, which returns the DDL status instead of the result

# `expire_snapshots` keeps the head of every branch when the table property
# `history.expire.min-snapshots-to-keep` is 0: https://github.com/ClickHouse/ClickHouse/issues/124333

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 --use_iceberg_metadata_files_cache=0"

# main: the table as ClickHouse writes it; branch: plus a branch `b` at the oldest snapshot;
# no_refs: no `refs`, so `current-snapshot-id` is the only head.
for mode in main branch no_refs; do
    TABLE="t_${CLICKHOUSE_DATABASE}_${mode}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
    rm -rf "${TABLE_PATH}"
    $CH --query "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS allow_experimental_iceberg_compaction = 0"
    for i in 1 2 3; do
        $CH --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (${i})"
    done

    # Sets the property, applies the mode, and prints the snapshot ids oldest first.
    ids=$(python3 - "${TABLE_PATH}metadata" "${mode}" <<'PY'
import json, os, re, sys
d, mode = sys.argv[1], sys.argv[2]
latest = max(int(m.group(1)) for f in os.listdir(d) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
meta = json.load(open(os.path.join(d, f"v{latest}.metadata.json")))
parent = {s["snapshot-id"]: s.get("parent-snapshot-id") for s in meta["snapshots"]}
chain = [meta["current-snapshot-id"]]
while parent.get(chain[-1]) in parent:
    chain.append(parent[chain[-1]])
chain.reverse()
meta.setdefault("properties", {})["history.expire.min-snapshots-to-keep"] = "0"
if mode == "branch":
    meta["refs"]["b"] = {"snapshot-id": chain[0], "type": "branch"}
elif mode == "no_refs":
    del meta["refs"]
json.dump(meta, open(os.path.join(d, f"v{latest + 1}.metadata.json"), "w"))
print(",".join(map(str, chain)))
PY
)

    echo "${mode}"
    $CH --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms')" | grep -E "deleted_(data_files|manifest_lists)_count"
    $CH --query "SELECT groupArray(x) FROM (SELECT x FROM ${TABLE} ORDER BY x)"
    # Positions (1 = oldest) of the snapshots that are left.
    $CH --query "SELECT arraySort(groupArray(indexOf([${ids}], snapshot_id))) FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
    $CH --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
done
