#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-parallel: the repro relies on a warmed entry in the server-global Iceberg metadata files
# cache surviving between the warm-up SELECT and the INSERT; non-group Iceberg reads or LRU
# eviction under parallel pressure drop it, so the `metadata-caches` group is not enough - it must
# run fully sequentially.
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel: `SYSTEM DROP ICEBERG METADATA CACHE` is used.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Report the INSERT outcome for every outcome, so a run that stops exercising the
# rejection path fails visibly instead of printing nothing.
insert_outcome() {
    local out
    if out=$(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
                --async_insert=0 --query "$1" 2>&1)
    then
        echo "NO_ERROR"
    else
        # The code name is the last parenthesised upper-case token before any stack trace.
        local code
        code=$(echo "$out" | grep -m1 '^Code: ' | sed 's/, Stack trace.*//' \
                   | grep -o '([A-Z][A-Z0-9_]*)' | tail -1 | tr -d '()')
        echo "${code:-UNPARSED_ERROR}"
    fi
}

# --- Scenario 1: top-level column rename -------------------------------------------------------
# The block carries a stale top-level name absent from the sink's force-fetched latest field-id map.
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"

# Warm the persistent metadata cache with the current schema {c0, c1}. Clear it first: a cache left
# at its entry bound by earlier tests would evict this entry again before the INSERT below reads it.
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null

# Another writer publishes a newer metadata version renaming c0 -> c9. The on-disk current schema
# becomes {c9, c1} while the cache still serves {c0, c1}.
python3 - "${TABLE_PATH}/metadata" <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
ns = json.loads(json.dumps(m['schemas'][0])); ns['schema-id'] = 1
for f in ns['fields']:
    if f['name'] == 'c0':
        f['name'] = 'c9'
m['schemas'].append(ns)
m['current-schema-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY

# A synchronous INSERT shaped by the stale schema (column c0) while the sink force-reads the latest
# schema {c9, c1}. Before the fix this aborted the server with a std::out_of_range logical error in
# Parquet::prepareColumnRecursive (unordered_map::at). After the fix it must be a clean query error.
insert_outcome "INSERT INTO ${TABLE} (c0, c1) SELECT 2, 'b'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

# --- Scenario 2: nested column rename inside a stable top-level field --------------------------
# A rename inside a Tuple keeps the top-level field id, so a top-level-only check would pass and the
# renamed leaf would be silently written without a field id. Validation must be recursive: the
# nested mismatch must also be rejected cleanly instead of writing a footer inconsistent with the
# latest Iceberg schema.
NTABLE="n_${CLICKHOUSE_DATABASE}_${RANDOM}"
NTABLE_PATH="${USER_FILES_PATH}/${NTABLE}/"
rm -rf "${NTABLE_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${NTABLE} (c0 Int32, t Tuple(a Int32)) ENGINE = IcebergLocal('${NTABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${NTABLE} SELECT 1, tuple(10)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${NTABLE}" > /dev/null

# Publish a newer metadata version renaming the nested field t.a -> t.b. The top-level field t keeps
# its field id; only the nested leaf changes name.
python3 - "${NTABLE_PATH}/metadata" <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
ns = json.loads(json.dumps(m['schemas'][0])); ns['schema-id'] = 1
for f in ns['fields']:
    if f['name'] == 't':
        for sub in f['type']['fields']:
            if sub['name'] == 'a':
                sub['name'] = 'b'
m['schemas'].append(ns)
m['current-schema-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY

insert_outcome "INSERT INTO ${NTABLE} (c0, t) SELECT 2, tuple(20)"

# The server must still be alive after both scenarios (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NTABLE}"
rm -rf "${NTABLE_PATH}" 2>/dev/null
