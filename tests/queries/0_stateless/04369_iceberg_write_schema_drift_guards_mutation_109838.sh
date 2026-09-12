#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - long: several full Iceberg table-lifecycle scenarios; exempts the 180s flaky-check cap.
# - no-parallel: the drift scenarios rely on the server-global Iceberg metadata cache staying
#   warm (or being dropped) at a precise point; a concurrent SYSTEM DROP ICEBERG METADATA CACHE
#   or LRU eviction would refresh the cached schema and the drift would stop firing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Part 2/5 of the Iceberg write/mutation/compaction schema-drift guard regression
# (issues #109835 / #109838): UPDATE/DELETE mutation (Mutations.cpp).
# Part 1 (INSERT sink) lives in 04365_iceberg_write_schema_drift_guards_insert_109838.sh.
# Part 3 (OPTIMIZE compaction - schema/field-id drift) lives in
# 04371_iceberg_write_schema_drift_guards_compaction_109838.sh.
# Part 4 (OPTIMIZE compaction - spec/leak/evolution) lives in
# 04372_iceberg_write_schema_drift_guards_compaction_spec_109838.sh.
# Part 5 (metadata edge cases) lives in 04373_iceberg_write_schema_drift_guards_metadata_109838.sh.
# The Iceberg write paths map input block columns positionally onto schema fields, so a stale
# attached table or malformed metadata could abort the server (field_ids[] out of bounds) or
# silently commit data files with the wrong names/types/field-ids. Each scenario asserts a clean
# query error (not an abort) and that the server stays alive.
# Each scenario gets a fresh table name+path (see reset), removing cross-scenario cache coupling.
# Mutation tables use Parquet: Iceberg UPDATE/DELETE are only supported for Parquet data files.

_scenario=0
TABLE="t_${CLICKHOUSE_DATABASE}_0"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

reset() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
    _scenario=$((_scenario + 1))
    TABLE="t_${CLICKHOUSE_DATABASE}_${_scenario}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
}

# Publish a new metadata version derived from the latest one, applying a python edit passed on stdin.
# The edit receives the parsed dict `m` and must leave the new version in `m`.
publish_next_metadata() {
    python3 - "${TABLE_PATH}/metadata" "$1"
}

# ============================================================================================
# UPDATE mutation (Mutations.cpp)
# ============================================================================================

# --- UPDATE same-width rename drift: rejected -------------------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rename_c0_to_c9_new_schema <<'PY'
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
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- DELETE same-width rename drift: rejected ------------------------------------------------
# A partitioned DELETE builds ChunkPartitioner from the current schema against the stale header.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c0) SETTINGS iceberg_format_version=2;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rename_c1_to_c9_new_schema <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
ns = json.loads(json.dumps(m['schemas'][0])); ns['schema-id'] = 1
for f in ns['fields']:
    if f['name'] == 'c1':
        f['name'] = 'c9'
m['schemas'].append(ns)
m['current-schema-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- UPDATE on a malformed table (current-schema-id resolves to no schema): rejected ----------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata current_schema_id_absent <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
m['current-schema-id'] = 999
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# --- UPDATE without drift: still works --------------------------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1;
SELECT c0, c1 FROM ${TABLE} ORDER BY c0;
"

# --- UPDATE across a same-layout partition-spec rebind: allowed ------------------------------
# Another engine may republish the identical partition layout under a new spec-id. The mutation
# guard must reject only a genuine layout change, not the id alone (Compaction.cpp compares the
# layout field-by-field). Data files stay tagged spec-id 0; default becomes spec-id 1 with the
# same fields, so position deletes still land in the right partitions. The UPDATE must succeed
# and supersede the old row.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c1) SETTINGS iceberg_format_version=2;
INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'a');
"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rebind_same_layout_new_spec_id <<'PY'
import json, os, sys, glob, re
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1)))
latest = vs[-1]
m = json.load(open(latest))
# Duplicate the current default spec's layout under a new spec-id, then make it the default.
spec0 = [s for s in m['partition-specs'] if s['spec-id'] == m['default-spec-id']][0]
new = json.loads(json.dumps(spec0)); new['spec-id'] = 1
m['partition-specs'].append(new)
m['default-spec-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(re.search(r'v(\d+)', os.path.basename(latest)).group(1)) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 --query "
ALTER TABLE ${TABLE} UPDATE c0 = 99 WHERE c1 = 'b';
SYSTEM DROP ICEBERG METADATA CACHE;
SELECT c0, c1 FROM ${TABLE} ORDER BY c1, c0;
"

# The server must still be alive after all scenarios (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

reset
