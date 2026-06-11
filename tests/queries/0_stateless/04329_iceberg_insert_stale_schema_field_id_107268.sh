#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

# Step 1: create + seed an IcebergLocal table with schema {c0, c1}.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"

# Step 2: warm the persistent metadata cache with the current schema {c0, c1}.
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null

# Step 3: another writer publishes a newer metadata version renaming c0 -> c9.
# The on-disk current schema becomes {c9, c1} while the cache still serves {c0, c1}.
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

# Step 4: a synchronous INSERT shaped by the stale schema (column c0) while the sink force-reads
# the latest schema {c9, c1}. Before the fix this aborted the server with a std::out_of_range
# logical error in Parquet::prepareColumnRecursive (unordered_map::at). After the fix it must be
# a clean query error.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 --async_insert=0 \
    --query "INSERT INTO ${TABLE} (c0, c1) SELECT 2, 'b'" 2>&1 | grep -oF "INCORRECT_DATA" | head -1

# Step 5: the server must still be alive (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null
