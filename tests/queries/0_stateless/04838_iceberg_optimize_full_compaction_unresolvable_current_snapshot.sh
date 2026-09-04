#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Split out of `04838_iceberg_optimize_manifest_unresolvable_current_snapshot` because this
# expectation only holds in the open-source build. `IcebergMetadata::optimize` reaches
# `compactIcebergTable` - and through it `getPlan`, where the guard lives - only when
# `CLICKHOUSE_CLOUD` is off; a cloud build defers to its own background compactor instead.
# The test is therefore listed in `tests/queries-no-private-tests.txt` in the private repository.
# The manifest path has no such branch, so its half of the original test stays build-independent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

# Drop the table before removing its directory, so an early exit cannot leave an attached
# table pointing at a path that no longer exists.
cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC" 2>/dev/null
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

INSERTS=$(for i in $(seq 0 34); do echo "INSERT INTO ${TABLE} VALUES (1, ${i});"; done)
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${INSERTS}"

# Make the metadata self-contradictory the way a mis-pruned `snapshots` list does: keep a live
# `current-snapshot-id` but drop that snapshot from `snapshots`.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json, sys
path = sys.argv[1]
meta = json.load(open(path))
current = meta["current-snapshot-id"]
assert current is not None and current >= 0, "expected a live current snapshot to remove"
meta["snapshots"] = [s for s in meta["snapshots"] if s["snapshot-id"] != current]
json.dump(meta, open(path, "w"))
PY

# Drop the in-memory metadata so the edited file is re-read.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --allow_repeated_settings --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}"

# `getHistory` drops the unresolvable head silently, and the rewrite then deletes the files only
# that head referenced, so this path has to fail before it starts.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

# The server is still alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
