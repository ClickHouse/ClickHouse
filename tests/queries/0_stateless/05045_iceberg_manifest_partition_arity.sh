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
# Substitutes one value in an Avro object-container-file header metadata map. The map is
# re-emitted as a single block; the sync marker and every data block byte are copied
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

path, key, new_value = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
buf = open(path, "rb").read()
assert buf[:4] == MAGIC, "not an Avro object container file"

pos, items = 4, []
while True:
    count, pos = read_long(buf, pos)
    if count == 0:
        break
    if count < 0:                         # negative count is followed by a block byte size
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
PY

SPEC_PQ='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"},{"field-id":1002,"name":"q","source-id":2,"transform":"identity"}]'
SPEC_P='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"}]'
# Arity-correct spec whose second field cannot be modelled: its source id is absent from
# the manifest schema, which is what Iceberg leaves behind when a partition source column
# is dropped. ClickHouse then omits that field from the partition key it builds.
SPEC_UNMODELLED='[{"field-id":1001,"name":"p","source-id":1,"transform":"identity"},{"field-id":1002,"name":"q","source-id":9999,"transform":"void"}]'

# One partition value group per file, so the table has three data files.
mk() { # mk <table> <partition-by>
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE $1 (p Int32, q Int32, v Int32)
        ENGINE = IcebergLocal('${ROOT}/$1/', 'Parquet') PARTITION BY ($2)"
    # An inline VALUES list still consumes stdin, which blocks until EOF if the caller left it open.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 \
        --query "INSERT INTO $1 VALUES (1,10,100),(1,10,101),(2,20,200),(2,20,201),(3,30,300)" < /dev/null
}

patch_spec() { # patch_spec <table> <spec json>
    for f in "${ROOT}"/"$1"/metadata/*.avro; do
        case "$(basename "$f")" in snap-*) continue;; esac
        python3 "${HELPER}" "$f" partition-spec "$2"
    done
}

# Whether the min/max index pruned any file. Read through log_comment so this probe cannot
# count itself, and as a boolean so a repeated query_log row cannot change the verdict.
min_max_pruned() { # min_max_pruned <table> <tag>
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --log_comment="${CLICKHOUSE_DATABASE}_$2" \
        --query "SELECT sum(v) FROM $1 WHERE v > 100000 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0
        FROM system.query_log
        WHERE log_comment = '${CLICKHOUSE_DATABASE}_$2' AND type = 'QueryFinish'"
}

echo '--- A0 spec arity matches the partition tuple ---'
mk t_ok "p, q"
# An unpopulated table would make every arm below pass without reaching the pruner.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count(), sum(v) FROM t_ok"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_ok WHERE p = 1"

echo '--- A1 manifest tuple longer than the spec ---'
mk t_long "p, q"
patch_spec t_long "${SPEC_P}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_long WHERE p = 1" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo '--- A2 manifest tuple shorter than the spec ---'
mk t_short "p"
patch_spec t_short "${SPEC_PQ}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_short WHERE p = 1" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo '--- A3 partition key narrower than the tuple is not a violation ---'
mk t_narrow "p, q"
patch_spec t_narrow "${SPEC_UNMODELLED}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_narrow WHERE p = 1"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(v) FROM t_narrow"

echo '--- A4 the server is still alive ---'
${CLICKHOUSE_CLIENT} --query "SELECT 1"

echo '--- A5 min/max pruning still applies when the partition key is narrower ---'
min_max_pruned t_ok min_max_control
min_max_pruned t_narrow min_max_narrow

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_ok SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_long SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_short SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_narrow SYNC"
