#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-disk-block-formats.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

DENSE="SELECT 'dense', count() FROM numbers(100000) WHERE number IN (SELECT number FROM numbers(100000))
SETTINGS log_comment = 'dense';"
DENSE_32_BIT="SELECT 'dense 32-bit', count() FROM numbers(150000)
WHERE toUInt32(number) IN (SELECT toUInt32(number) FROM numbers(150000) WHERE number % 3 != 0)
SETTINGS log_comment = 'dense 32-bit';"
HASHES="SELECT 'hashes', count() FROM numbers(100000) WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(100000))
SETTINGS log_comment = 'hashes';"
HASHES_FILLING_BLOCKS="SELECT 'hashes filling their blocks', count() FROM numbers(1024)
WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(1024))
SETTINGS log_comment = 'hashes filling their blocks';"
DENSE_AND_HASHES="SELECT 'dense and hashes', count() FROM numbers(200000)
WHERE if(number < 100000, number, bitOr(cityHash64(number), 0x8000000000000000))
    IN (SELECT if(number < 100000, number, bitOr(cityHash64(number), 0x8000000000000000)) FROM numbers(200000))
SETTINGS log_comment = 'dense and hashes';"
TUPLES="SELECT 'tuples', count() FROM numbers(100000)
WHERE (intDiv(number, 1000), toUInt32(number % 1000)) IN (SELECT intDiv(number, 1000), toUInt32(number % 1000) FROM numbers(100000))
SETTINGS log_comment = 'tuples';"
STRINGS="SELECT 'strings', count() FROM numbers(100000) WHERE toString(number) IN (SELECT toString(number) FROM numbers(100000))
SETTINGS log_comment = 'strings';"

# A set on disk packs the gaps between its keys when that at least halves its blocks. A block of packed gaps
# holds up to 4096 keys, and an unpacked block holds 4096 bytes of keys. Each query probes every key of its
# set in one chunk, so it reads every block once, and the number of blocks that it reads shows how the set
# stores its keys. Dense numbers pack. So do `UInt32` numbers with gaps of one and two, which use 32-bit
# offsets and the SIMD layout of `PFor`, and tuples of slowly changing numbers, whose keys take 16 bytes.
# Hashes do not pack, and neither do the disk keys of strings, whose low 96 bits are hashes. A set of dense
# numbers and hashes takes blocks of both kinds. The 1024 hashes of a set fill exactly two blocks, so the
# last key completes the last block. Each query also counts the keys that it finds.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" \
    --max_bytes_before_external_set 1 --max_threads 1 --max_block_size 1000000 <<SQL
${DENSE}
${DENSE_32_BIT}
${HASHES}
${HASHES_FILLING_BLOCKS}
${DENSE_AND_HASHES}
${TUPLES}
${STRINGS}

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetReadBlocks']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL

# The blocks must not depend on how the merge splits the keys into batches. These sets are written again in
# batches of 1000 keys, so that their blocks of each kind, and the switches between the kinds, span batches.
# They take the same blocks. Each set logs a trace line with its number of blocks when it is written.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/batches" --max_bytes_before_external_set 1 --max_threads 1 --max_block_size 1000 \
    --send_logs_level trace <<< "${DENSE} ${HASHES} ${HASHES_FILLING_BLOCKS} ${DENSE_AND_HASHES}" 2>&1 >/dev/null \
    | grep -o -E 'Created set on disk with [0-9]+ keys of [0-9]+ bytes, [0-9]+ blocks'
