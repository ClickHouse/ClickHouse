#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/89037:
# `IcebergMetadata::getHistory` read the OPTIONAL snapshot-summary metrics
# `added-files-size` and `changed-partition-count` with an unguarded
# `Poco::JSON::Object::getValue<Int32>`. An absent key yields an empty
# `Poco::Dynamic::Var`, so `Var::convert<Int32>` threw
# `Invalid access: Can not convert empty value` and `OPTIMIZE TABLE` failed on a
# spec-conforming lake (the reporter's lake was written by a Spark `MERGE`).
# Both fields are optional per the Iceberg spec, so omitting them is legal.
#
# The summary metrics are now parsed by `SnapshotSummary::fromJSON`, where every
# metric goes through a `has()` check and defaults to 0.
#
# ClickHouse's own writer always emits both metrics, so the lake is built through
# supported SQL and the two fields are then removed by publishing a NEW metadata
# version. The strip must NOT rewrite the existing version in place: metadata JSON
# is cached (`use_iceberg_metadata_files_cache` defaults to 1), so an in-place edit
# of an already-read file is not re-read and the test would pass even without the
# fix. A path that was never read cannot have a cached entry.
#
# Only those two fields are removed. `added-data-files` must stay, because
# `checkIfIcebergHistorySupported` runs after `getHistory` and rejects an append
# with 0 added files, and the snapshots must stay `append`, because
# `tryGetAppendUpdate` rejects other operation types.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (x Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

# Two `append` snapshots.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (2)"

# Publish a new metadata version whose snapshot summaries omit the two optional metrics.
python3 - "${TABLE_PATH}metadata" <<'PYEOF'
import json
import os
import re
import sys

metadata_dir = sys.argv[1]
versions = []
for name in os.listdir(metadata_dir):
    match = re.fullmatch(r"v(\d+)\.metadata\.json", name)
    if match:
        versions.append((int(match.group(1)), name))
assert versions, "no vN.metadata.json found in %s" % metadata_dir

latest_version, latest_name = max(versions)
with open(os.path.join(metadata_dir, latest_name)) as handle:
    meta = json.load(handle)

assert meta.get("snapshots"), "snapshots must be present after INSERT"

# Removing these must not silently become a no-op if the metadata layout changes.
removed = 0
for snapshot in meta["snapshots"]:
    summary = snapshot.get("summary", {})
    assert summary.get("operation") == "append", "fixture requires append snapshots, got %r" % summary.get("operation")
    assert "added-data-files" in summary, "added-data-files must be kept"
    for field in ("added-files-size", "changed-partition-count"):
        if field in summary:
            del summary[field]
            removed += 1
assert removed > 0, "no optional summary metric was removed: the fixture is vacuous"
print("removed optional summary metrics:", removed)

# A NEW version, never an in-place rewrite: the previous version is already cached.
new_path = os.path.join(metadata_dir, "v%d.metadata.json" % (latest_version + 1))
tmp_path = new_path + ".tmp"
with open(tmp_path, "w") as handle:
    json.dump(meta, handle, indent=4)
os.rename(tmp_path, new_path)
PYEOF

# On the cloud build `OPTIMIZE` for Iceberg is gated by a member flag rather than
# the query-level `allow_experimental_iceberg_compaction` setting, and that path
# never calls `getHistory`, so it reports a user-facing exception instead.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# This used to fail with `Invalid access: Can not convert empty value`.
# Capture the client's stderr so a regular user-facing exception on the cloud
# build does not trip the "having stderror" check, then classify the outcome.
OPTIMIZE_ERR=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if echo "${OPTIMIZE_ERR}" | grep -qF 'Can not convert empty value'; then
    # The regression: reading an absent optional metric must never throw.
    echo "FAIL: OPTIMIZE hit the missing-optional-summary-metric conversion"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    # Cloud routes `OPTIMIZE` through a background path; a user-facing exception is expected.
    echo "OPTIMIZE did not hit the missing-optional-summary-metric conversion"
elif [[ -n "${OPTIMIZE_ERR}" ]]; then
    # The open-source build runs the synchronous compaction path and must succeed,
    # otherwise an unrelated failure would pass this check silently.
    echo "FAIL: OPTIMIZE failed on the open-source build: ${OPTIMIZE_ERR}"
else
    echo "OPTIMIZE did not hit the missing-optional-summary-metric conversion"
fi

# `StorageSystemIcebergHistory::fillData` swallows a `getHistory` exception, so the
# regression drops the rows instead of reporting an error. This oracle therefore
# discriminates by row count, and covers the cloud build too.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
"

${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${TABLE} ORDER BY x"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
