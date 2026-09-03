#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_ibp"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

HELPER="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_avro_header.py"
cat > "${HELPER}" <<'PY'
# Substitutes one value in the header metadata map of every Avro object-container file given.
# The map is re-emitted as a single block; the sync marker and every data block byte are copied
# verbatim, so no block length or record has to be recomputed.
import sys

MAGIC = b"Obj\x01"

def read_long(b, p):                      # Avro long: zigzag varint
    shift = acc = 0
    while True:
        c = b[p]; p += 1
        acc |= (c & 0x7F) << shift
        if not c & 0x80:
            return (acc >> 1) ^ -(acc & 1), p
        shift += 7

def write_long(n):
    z = (n << 1) if n >= 0 else ((-n) << 1) - 1
    out = bytearray()
    while True:
        c = z & 0x7F; z >>= 7
        out.append(c | 0x80 if z else c)
        if not z:
            return bytes(out)

def read_bytes(b, p):
    n, p = read_long(b, p)
    return b[p:p + n], p + n

def patch(path, key, new_value):
    buf = open(path, "rb").read()
    assert buf[:4] == MAGIC, "not an Avro object container file"

    pos, items = 4, []
    while True:
        count, pos = read_long(buf, pos)
        if count == 0:
            break
        if count < 0:                     # negative count is followed by a block byte size
            count = -count
            _, pos = read_long(buf, pos)
        for _ in range(count):
            k, pos = read_bytes(buf, pos)
            v, pos = read_bytes(buf, pos)
            items.append((k, v))
    assert any(k == key for k, _ in items), "header key absent"

    out = bytearray(MAGIC) + write_long(len(items))
    for k, v in items:
        v = new_value if k == key else v
        out += write_long(len(k)) + k + write_long(len(v)) + v
    out += write_long(0) + buf[pos:]
    open(path, "wb").write(bytes(out))

key, new_value = sys.argv[1].encode(), sys.argv[2].encode()
for arg in sys.argv[3:]:
    patch(arg, key, new_value)
PY

SPEC_PQ='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"},{"field-id":1002,"name":"q","source-id":2,"transform":"identity"}]'
SPEC_P='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"}]'
# Arity-correct spec whose second field cannot be modelled: its source id is absent from
# the manifest schema, which is what Iceberg leaves behind when a partition source column
# is dropped. ClickHouse then omits that field from the partition key it builds.
SPEC_UNMODELLED='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"},{"field-id":1002,"name":"q","source-id":9999,"transform":"void"}]'
# Same, but the unmodellable field leads. The partition key then starts at the tuple's
# second element, so an implementation that truncated the tuple instead of skipping the
# evaluation would pair the key with the wrong value.
SPEC_UNMODELLED_LEAD='[{"field-id":1001,"name":"q","source-id":9999,"transform":"void"},{"field-id":1002,"name":"p","source-id":1,"transform":"identity"}]'
# No field is modellable, so no partition key is built at all. One spec has the wrong arity
# for the tuple and one has the right arity, so the two together pin that the spec field
# count is read for such a manifest rather than assumed to be zero.
SPEC_NOKEY='[{"field-id":1001,"name":"p","source-id":9999,"transform":"void"}]'
SPEC_NOKEY_OK='[{"field-id":1001,"name":"p","source-id":9999,"transform":"void"},{"field-id":1002,"name":"q","source-id":9998,"transform":"void"}]'

# One partition value group per file, so each table has three data files. Background Iceberg
# compaction, which the cloud build runs off a member flag rather than a query setting, would
# rewrite the very manifests this test patches, so it is pinned off per table.
mk() { # mk <table> <partition-by>
    cat <<SQL
CREATE TABLE $1 (p Int32, q Int32, v Int32)
ENGINE = IcebergLocal('${ROOT}/$1/', 'Parquet') PARTITION BY ($2)
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO $1 VALUES (1,10,100),(1,10,101),(2,20,200),(2,20,201),(3,30,300);
SQL
}

patch_spec() { # patch_spec <table> <spec json>
    local files=()
    for f in "${ROOT}"/"$1"/metadata/*.avro; do
        case "$(basename "$f")" in snap-*) continue;; esac
        files+=("$f")
    done
    python3 "${HELPER}" partition-spec "$2" "${files[@]}"
}

# Runs each probe query under its own log_comment and flushes the log, in one client call.
# Reading a counter back through log_comment keeps a probe from counting itself, and the
# boolean verdict below keeps a repeated query_log row from changing it.
probes() { # probes <tag> <query> [<tag> <query>]...
    local sql=""
    while [ "$#" -gt 0 ]; do
        sql="${sql}$2 SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_$1' FORMAT Null;
"
        shift 2
    done
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "${sql}SYSTEM FLUSH LOGS query_log;"
}

# One line per tag, in the order given: whether <counter> saw any file pruned by that probe.
report() { # report <counter> <tag>...
    local counter=$1 tags="" t
    shift
    for t in "$@"; do tags="${tags:+${tags}, }'${CLICKHOUSE_DATABASE}_${t}'"; done
    ${CLICKHOUSE_CLIENT} --query "
        SELECT max(ProfileEvents['${counter}']) > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN (${tags})
        GROUP BY log_comment
        ORDER BY indexOf([${tags}], log_comment)
        SETTINGS enable_parallel_replicas = 0"
}

# Every table is built in one client call and every spec patched before the arms run: the
# private stateless jobs cap this test at 180s, where a client start costs more than a query.
# An inline VALUES list still consumes stdin, which blocks until EOF if the caller left it open.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "
$(mk t_ok 'p, q')
$(mk t_long 'p, q')
$(mk t_short 'p')
$(mk t_narrow 'p, q')
$(mk t_nokey 'p, q')
$(mk t_nokey_ok 'p, q')
$(mk t_narrow_lead 'q, p')
" < /dev/null

patch_spec t_long "${SPEC_P}"
patch_spec t_short "${SPEC_PQ}"
patch_spec t_narrow "${SPEC_UNMODELLED}"
patch_spec t_nokey "${SPEC_NOKEY}"
patch_spec t_nokey_ok "${SPEC_NOKEY_OK}"
patch_spec t_narrow_lead "${SPEC_UNMODELLED_LEAD}"

echo '--- A0 spec arity matches the partition tuple ---'
# An unpopulated table would make every arm below pass without reaching the pruner.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count(), sum(v) FROM t_ok;
    SELECT sum(v) FROM t_ok WHERE p = 1;"

echo '--- A1 manifest tuple longer than the spec ---'
# One call per query: the client stops at the first error, so a batch would report only one.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_long WHERE p = 1" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1
# Such a manifest is rejected however it is read: a read that builds no filter, and one with
# pruning off, both consume the tuple to match delete files to the data files they cover.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count() FROM t_long" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --query "SELECT sum(v) FROM t_long WHERE p = 1 SETTINGS use_iceberg_partition_pruning = 0" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo '--- A2 manifest tuple shorter than the spec ---'
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_short WHERE p = 1" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo '--- A3 partition key narrower than the tuple is not a violation ---'
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT sum(v) FROM t_narrow WHERE p = 1;
    SELECT sum(v) FROM t_narrow;"

echo '--- A4 the server is still alive ---'
${CLICKHOUSE_CLIENT} --query "SELECT 1"

echo '--- A5 min/max pruning still applies when the partition key is narrower ---'
probes min_max_control "SELECT sum(v) FROM t_ok WHERE v > 100000" \
       min_max_narrow "SELECT sum(v) FROM t_narrow WHERE v > 100000"
report IcebergMinMaxIndexPrunedFiles min_max_control min_max_narrow

echo '--- A6 the tuple arity is validated even when no partition key is modelled ---'
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_nokey WHERE p = 1" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_nokey_ok WHERE p = 1"

echo '--- A7 partition pruning happens, and stops when the key is narrower ---'
probes partition_control "SELECT sum(v) FROM t_ok WHERE p = 1" \
       partition_narrow "SELECT sum(v) FROM t_narrow WHERE p = 1"
report IcebergPartitionPrunedFiles partition_control partition_narrow

echo '--- A8 a leading unmodellable field is not realigned onto the wrong value ---'
# The one query serves as both the value check and the pruning probe.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT sum(v) FROM t_narrow_lead WHERE p = 1 SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_partition_narrow_lead';
    SYSTEM FLUSH LOGS query_log;"
report IcebergPartitionPrunedFiles partition_narrow_lead

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_ok SYNC;
    DROP TABLE IF EXISTS t_long SYNC;
    DROP TABLE IF EXISTS t_short SYNC;
    DROP TABLE IF EXISTS t_narrow SYNC;
    DROP TABLE IF EXISTS t_nokey SYNC;
    DROP TABLE IF EXISTS t_nokey_ok SYNC;
    DROP TABLE IF EXISTS t_narrow_lead SYNC;"
