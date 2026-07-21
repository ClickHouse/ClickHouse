#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - long: many full Iceberg table-lifecycle scenarios; exempts the 180s flaky-check cap.
# - no-parallel: the drift scenarios rely on the server-global Iceberg metadata cache staying
#   warm (or being dropped) at a precise point; a concurrent SYSTEM DROP ICEBERG METADATA CACHE
#   or LRU eviction would refresh the cached schema and the drift would stop firing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Part 2/2 of the Iceberg write/mutation/compaction schema-drift guard regression
# (issues #109835 / #109838): first-load missing partition-spec, INSERT metadata-conflict
# retry, and UPDATE across a partition-spec evolution.
# Part 1 lives in 04365_iceberg_write_schema_drift_guards_109838.sh.
# Each scenario gets a fresh table name+path (see reset), removing cross-scenario cache coupling.

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
# First metadata load: missing default-spec-id partition-specs entry (INSERT / UPDATE / DELETE)
# ============================================================================================
# The default-spec-id -> partition-specs lookup is unchecked on the first load; a missing entry
# leaves partititon_spec null and INSERT/UPDATE/DELETE would dereference it. Craft such a table
# by pointing default-spec-id at a spec id absent from partition-specs, then assert each path
# rejects it cleanly rather than aborting.
make_missing_spec_table() {
    reset
    # Unpinned: the write path loads the latest metadata version, so a crafted latest v3 whose
    # default-spec-id points at an absent partition-specs entry leaves partititon_spec null.
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}')"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
    publish_next_metadata default_spec_id_absent <<'PY'
import json, os, sys, glob, re
md = sys.argv[1]
f = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
           key=lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1)))[-1]
m = json.load(open(f))
m['default-spec-id'] = 999
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
ver = int(re.search(r'v(\d+)', os.path.basename(f)).group(1))
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{ver + 1}.metadata.json'))
PY
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
}

make_missing_spec_table
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
    --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

make_missing_spec_table
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE ${TABLE} UPDATE c1 = 'z' WHERE c0 = 1" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

make_missing_spec_table
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# ============================================================================================
# INSERT metadata-conflict retry (initializeMetadata / cleanup(true))
# ============================================================================================
# Pin the table to a stale metadata version (v1) so every INSERT targets v2. The first INSERT
# writes v2; an external v3 carrying drift is published and the cache dropped. The next INSERT
# still targets v2 (present), enters cleanup(true), re-reads the real latest (v3), and must
# reject any semantics-affecting change before reusing the buffered files.

# --- retry: default-spec-id change (schema id unchanged) -> NOT_IMPLEMENTED --------------------
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path='metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
publish_next_metadata retry_default_spec_change <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
m['partition-specs'].append({'spec-id': 1, 'fields': []})
m['default-spec-id'] = 1
m['last-partition-id'] = m.get('last-partition-id', 999) + 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
    --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 | grep -oF "NOT_IMPLEMENTED" | head -1

# --- retry: same-spec-id structural rebind -> NOT_IMPLEMENTED ----------------------------------
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY (c0) SETTINGS iceberg_metadata_file_path='metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
publish_next_metadata retry_same_spec_rebind <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
for s in m['partition-specs']:
    if s['spec-id'] == m['default-spec-id']:
        for f in s['fields']:
            if f['name'] == 'c0':
                f['transform'] = 'bucket[4]'
                f['name'] = 'c0_bucket'
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
    --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 | grep -oF "NOT_IMPLEMENTED" | head -1

# --- retry: dropped partition-specs entry for current default-spec-id -> ICEBERG_SPEC_VIOLATION
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY (c0) SETTINGS iceberg_metadata_file_path='metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
publish_next_metadata retry_drop_spec_entry <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
default_id = m['default-spec-id']
m['partition-specs'] = [s for s in m['partition-specs'] if s['spec-id'] != default_id]
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
    --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# --- retry: same-schema-id content rebind -> ICEBERG_SPECIFICATION_VIOLATION -------------------
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path='metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
publish_next_metadata retry_same_schema_id_rebind <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
for s in m['schemas']:
    if s['schema-id'] == m['current-schema-id']:
        for f in s['fields']:
            if f['name'] == 'c0':
                f['name'] = 'c9'
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
    --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# --- retry: no semantics-affecting drift -> still succeeds -------------------------------------
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path='metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 1, 'a'"
publish_next_metadata retry_noop <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT 2, 'b'" 2>&1 \
    | grep -qF "Exception" && echo "UNEXPECTED_ERROR" || echo "INSERT_OK"

# ============================================================================================
# UPDATE across partition-spec evolution: rejected
# ============================================================================================
# Data was written under the original spec; a sibling engine evolved the default spec. Position
# deletes are partitioned by the current spec, so they would never match rows written under the
# old spec and would silently fail to supersede them (rare row duplication under the flaky check).
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'k'), (2, 'k'), (3, 'k')"
publish_next_metadata evolve_spec_to_c0 <<'PY'
import json, os, sys, glob, re
md = sys.argv[1]
f = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
           key=lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1)))[-1]
m = json.load(open(f))
m['partition-specs'].append({'spec-id': 1, 'fields': [
    {'field-id': 1000, 'name': 'c0', 'source-id': 1, 'transform': 'identity'}]})
m['default-spec-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
ver = int(re.search(r'v(\d+)', os.path.basename(f)).group(1))
out = os.path.join(md, f'v{ver + 1}.metadata.json')
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w')); os.rename(tmp, out)
PY
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} UPDATE c1 = 'z' WHERE c0 IN (1, 2, 3)" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# --- Sanity: a partitioned UPDATE spanning >1 partition (per-partition slicing) still works ----
# Without per-partition slicing the mutated block would be written into every per-partition file.
reset
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c0)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} UPDATE c1 = 'z' WHERE c0 IN (1, 2)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# The server must still be alive after all scenarios (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

reset