#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-object-storage
# no-shared-merge-tree: uses ReplicatedMergeTree and corrupts the part on the local filesystem
# no-object-storage: the test edits the part file (data.packed) directly on the local disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS packed_fetch_src SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS packed_fetch_dst SYNC"

# Source holds a packed part; destination fetches it with ALTER ... FETCH PART.
# min_bytes_for_full_part_storage forces packed storage (the whole part in a single data.packed).
${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 -q "
CREATE TABLE packed_fetch_src (a UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/packed_fetch', 'src') ORDER BY a
SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, old_parts_lifetime = 100000;

CREATE TABLE packed_fetch_dst (a UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/packed_fetch_dst', 'dst') ORDER BY a
SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, old_parts_lifetime = 100000;

INSERT INTO packed_fetch_src VALUES (1, 'hello'), (2, 'world');
"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'packed_fetch_src' AND active")

# Sanity check: the part must actually be packed for this test to be meaningful.
${CLICKHOUSE_CLIENT} -q "SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 'packed_fetch_src' AND active"

# Corrupt a byte inside a column data file (.bin) of the single data.packed archive. checksums.txt
# stays intact, so the part is still loadable but its contents no longer match the checksums it
# advertises. Locate the file by the .bin extension, not by an exact stem: the on-disk stem is the
# column name only when it is short enough; with replace_long_file_name_to_hash and a small
# max_file_name_length (both randomized by CI) the stem is replaced by its hash, but the .bin
# extension is kept. Pick the largest .bin so the 4 bytes land squarely inside a checksummed region.
read -r BIN_OFFSET BIN_SIZE < <(${CLICKHOUSE_BINARY} packed-io -i "${DATA_PATH}data.packed" --list 2>/dev/null | awk '$1 ~ /\.bin$/ { print $3, $4 }' | sort -k2 -n | tail -1)
printf '\xAA\xBB\xCC\xDD' | dd of="${DATA_PATH}data.packed" bs=1 seek=$((BIN_OFFSET + BIN_SIZE / 2)) count=4 conv=notrunc 2>/dev/null

# Fetching the corrupted packed part must be rejected by checksum verification, exactly as it is for
# full part storage. If verification is skipped, the corrupted part is silently accepted into
# detached/ and would be propagated across replicas.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE packed_fetch_dst FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/${CLICKHOUSE_DATABASE}/packed_fetch'" 2>/dev/null

echo "detached parts fetched: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'packed_fetch_dst'")"

${CLICKHOUSE_CLIENT} -q "DROP TABLE packed_fetch_src SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE packed_fetch_dst SYNC"
