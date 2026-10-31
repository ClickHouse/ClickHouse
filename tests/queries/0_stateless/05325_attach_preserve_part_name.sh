#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
mkdir -p tmp
tmp_dir=$(mktemp -d tmp/05325_attach_preserve.XXXXXX)

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS preserve_destination SYNC;
        DROP TABLE IF EXISTS preserve_source SYNC;
        DROP TABLE IF EXISTS preserve_import SYNC;
    "
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

expect_error()
{
    local code="$1"
    local query="$2"
    if ${CLICKHOUSE_CLIENT} --query "$query" >"${tmp_dir}/answer" 2>"${tmp_dir}/error"; then
        echo "Expected ${code}: ${query}"
        exit 1
    fi
    if ! grep -q "$code" "${tmp_dir}/error"; then
        cat "${tmp_dir}/error"
        exit 1
    fi
    echo "$code"
}

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS preserve_destination SYNC;
    DROP TABLE IF EXISTS preserve_source SYNC;
    DROP TABLE IF EXISTS preserve_import SYNC;
    CREATE TABLE preserve_source (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05325_preserve', 'source') ORDER BY tuple();
    INSERT INTO preserve_source VALUES (111);
    INSERT INTO preserve_source VALUES (222);
    OPTIMIZE TABLE preserve_source FINAL;
    ALTER TABLE preserve_source UPDATE x = x + 1 WHERE x = 111 SETTINGS mutations_sync = 1;
    SYSTEM STOP MERGES preserve_source;
    INSERT INTO preserve_source VALUES (333);
    SYSTEM SYNC REPLICA preserve_source;
    SYSTEM STOP REPLICATED SENDS preserve_source;
    CREATE TABLE preserve_destination (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05325_preserve', 'destination') ORDER BY tuple();
    SYSTEM STOP FETCHES preserve_destination;
    SYSTEM STOP MERGES preserve_destination;
    CREATE TABLE preserve_import (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO preserve_import VALUES (999);
"

source_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'preserve_source'")
destination_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'preserve_destination'")
import_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'preserve_import'")
part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_source' AND active AND rows = 2")
other_part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_source' AND active AND rows = 1")
import_part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_import' AND active")
zk_path=$(${CLICKHOUSE_CLIENT} --query "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'preserve_source'")

stage_part()
{
    local source_part="$1"
    local target_part="$2"
    mkdir -p "${destination_path}/detached"
    cp -a "${source_path}/${source_part}" "${destination_path}/detached/tmp_${target_part}"
    mv "${destination_path}/detached/tmp_${target_part}" "${destination_path}/detached/${target_part}"
}

# The header exists on the source but not on the destination. No peer can send data.
${CLICKHOUSE_CLIENT} --query "SELECT 'metadata_before', count() FROM system.zookeeper WHERE path = '${zk_path}/replicas/destination/parts' AND name = '${part}'"
log_version=$(${CLICKHOUSE_CLIENT} --query "SELECT cversion FROM system.zookeeper WHERE path = '${zk_path}' AND name = 'log'")
block_version=$(${CLICKHOUSE_CLIENT} --query "SELECT cversion FROM system.zookeeper WHERE path = '${zk_path}/block_numbers' AND name = 'all'")
stage_part "$part" "$part"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_destination ATTACH PART '${part}' SETTINGS attach_preserve_part_name = 1"

${CLICKHOUSE_CLIENT} --multiquery --query "
    SELECT 'restored', count(), sum(x) FROM preserve_destination;
    SELECT 'same_identity',
        (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_source' AND active AND name = '${part}')
        =
        (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_destination' AND active AND name = '${part}');
    SELECT 'metadata_after', count() FROM system.zookeeper WHERE path = '${zk_path}/replicas/destination/parts' AND name = '${part}';
    SELECT 'no_shared_log_entry', cversion = ${log_version} FROM system.zookeeper WHERE path = '${zk_path}' AND name = 'log';
    SELECT 'no_new_block_number', cversion = ${block_version} FROM system.zookeeper WHERE path = '${zk_path}/block_numbers' AND name = 'all';
    SELECT 'source_unchanged', count(), sum(x) FROM preserve_source;
"

# A valid but different part must not be accepted as the required identity.
stage_part "$part" "$other_part"
expect_error CHECKSUM_DOESNT_MATCH "ALTER TABLE preserve_destination ATTACH PART '${other_part}' SETTINGS attach_preserve_part_name = 1"
${CLICKHOUSE_CLIENT} --query "SELECT 'unchanged_after_rejection', count(), sum(x) FROM preserve_destination"
${CLICKHOUSE_CLIENT} --query "SELECT 'mismatched_seed_preserved', count() = 1 FROM system.detached_parts WHERE database = currentDatabase() AND table = 'preserve_destination' AND name = '${other_part}'"
rm -rf "${destination_path}/detached/${other_part}"
stage_part "$other_part" "$other_part"
python3 - "${destination_path}/detached/${other_part}" <<'PY'
from pathlib import Path
import sys

part_file = next(Path(sys.argv[1]).glob("*.bin"))
with part_file.open("r+b") as file:
    byte = file.read(1)
    assert byte
    file.seek(0)
    file.write(bytes([byte[0] ^ 1]))
PY
if ${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_destination ATTACH PART '${other_part}' SETTINGS attach_preserve_part_name = 1" \
    >"${tmp_dir}/answer" 2>"${tmp_dir}/error"; then
    echo "Corrupt candidate was accepted"
    exit 1
fi
echo "corrupt_candidate_rejected"
${CLICKHOUSE_CLIENT} --query "SELECT 'unchanged_after_corruption', count(), sum(x) FROM preserve_destination"
${CLICKHOUSE_CLIENT} --query "SELECT 'corrupt_seed_preserved', count() = 1 FROM system.detached_parts WHERE database = currentDatabase() AND table = 'preserve_destination' AND name = '${other_part}'"
rm -rf "${destination_path}/detached/${other_part}"
stage_part "$other_part" "$other_part"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_destination ATTACH PART '${other_part}' SETTINGS attach_preserve_part_name = 1"

# Existing active data must not be overwritten or imported a second time.
stage_part "$part" "$part"
expect_error DUPLICATE_DATA_PART "ALTER TABLE preserve_destination ATTACH PART '${part}' SETTINGS attach_preserve_part_name = 1"
rm -rf "${destination_path}/detached/${part}"
expect_error BAD_ARGUMENTS "ALTER TABLE preserve_destination ATTACH PARTITION tuple() SETTINGS attach_preserve_part_name = 1"
expect_error NOT_IMPLEMENTED "ALTER TABLE preserve_import ATTACH PART '${import_part}' SETTINGS attach_preserve_part_name = 1"

# With the option disabled, ordinary attachment still allocates a new identity and replicates it.
cp -a "${import_path}/${import_part}" "${destination_path}/detached/${import_part}"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_destination ATTACH PART '${import_part}'"
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM START REPLICATED SENDS preserve_source;
    SYSTEM START FETCHES preserve_destination;
    SYSTEM SYNC REPLICA preserve_destination;
    SYSTEM SYNC REPLICA preserve_source;
    SELECT 'ordinary_attachment', count(), sum(x) FROM preserve_destination;
    SELECT 'peer_received_import', count(), sum(x) FROM preserve_source;
    SELECT 'ordinary_allocates_block', cversion > ${block_version} FROM system.zookeeper WHERE path = '${zk_path}/block_numbers' AND name = 'all';
"
