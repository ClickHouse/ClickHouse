#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
mkdir -p tmp
tmp_dir=$(mktemp -d tmp/05326_attach_preserve.XXXXXX)
insert_pid=""
query_id="attach_preserve_quorum_${CLICKHOUSE_DATABASE}_$$"

cleanup()
{
    if [[ -n "$insert_pid" ]]; then
        ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${query_id}' SYNC" >/dev/null
        wait "$insert_pid" || true
    fi
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS preserve_safe_destination SYNC;
        DROP TABLE IF EXISTS preserve_safe_source SYNC;
        DROP TABLE IF EXISTS preserve_safe_plain SYNC;
    "
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

expect_error()
{
    local expected="$1"
    local query="$2"
    if ${CLICKHOUSE_CLIENT} --query "$query" >"${tmp_dir}/answer" 2>"${tmp_dir}/error"; then
        echo "Unexpected success: ${query}"
        exit 1
    fi
    grep -q "$expected" "${tmp_dir}/error" || { cat "${tmp_dir}/error"; exit 1; }
    echo "$expected"
}

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS preserve_safe_destination SYNC;
    DROP TABLE IF EXISTS preserve_safe_source SYNC;
    DROP TABLE IF EXISTS preserve_safe_plain SYNC;
    CREATE TABLE preserve_safe_source (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05326_preserve', 'source') ORDER BY tuple();
    CREATE TABLE preserve_safe_destination (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05326_preserve', 'destination') ORDER BY tuple();
    SYSTEM STOP FETCHES preserve_safe_destination;
    SYSTEM STOP MERGES preserve_safe_source;
    SYSTEM STOP MERGES preserve_safe_destination;
    SYSTEM STOP REPLICATED SENDS preserve_safe_source;
"
source_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'preserve_safe_source'")
destination_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'preserve_safe_destination'")
zk_path=$(${CLICKHOUSE_CLIENT} --query "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'preserve_safe_source'")
mkdir -p "${destination_path}/detached"

for parallel in 0 1; do
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
        INSERT INTO preserve_safe_source
        SETTINGS insert_quorum = 2, insert_quorum_parallel = ${parallel}, insert_quorum_timeout = 60000,
                 insert_keeper_fault_injection_probability = 0, async_insert = 0
        VALUES ($((parallel + 1)))" >"${tmp_dir}/insert_answer" 2>"${tmp_dir}/insert_error" &
    insert_pid=$!
    quorum_path="${zk_path}/quorum"
    quorum_filter="name = 'status'"
    if [[ "$parallel" == 1 ]]; then
        quorum_path="${zk_path}/quorum/parallel"
        quorum_filter="1"
    fi
    pending=0
    for _ in $(seq 1 120); do
        pending=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.zookeeper WHERE path = '${quorum_path}' AND ${quorum_filter}")
        [[ "$pending" == 1 ]] && break
        sleep 0.1
    done
    [[ "$pending" == 1 ]]

    # A merged seed must also acknowledge the parallel quorum part that it covers.
    if [[ "$parallel" == 1 ]]; then
        ${CLICKHOUSE_CLIENT} --multiquery --query "
            SYSTEM START MERGES preserve_safe_source;
            OPTIMIZE TABLE preserve_safe_source FINAL;
            SYSTEM STOP MERGES preserve_safe_source;
        "
    fi
    part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_safe_source' AND active")
    cp -a "${source_path}/${part}" "${destination_path}/detached/${part}"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_safe_destination ATTACH PART '${part}' SETTINGS attach_preserve_part_name = 1"
    if ! wait "$insert_pid"; then
        cat "${tmp_dir}/insert_error"
        exit 1
    fi
    insert_pid=""
    ${CLICKHOUSE_CLIENT} --query "SELECT 'quorum_restored', ${parallel}, count(), sum(x) FROM preserve_safe_destination"
done

${CLICKHOUSE_CLIENT} --query "INSERT INTO preserve_safe_source VALUES (3)"
part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'preserve_safe_source' AND active AND rows = 1")
expect_error BAD_DATA_PART_NAME "ALTER TABLE preserve_safe_destination ATTACH PART '${part}' SETTINGS attach_preserve_part_name = 1"
cp -a "${source_path}/${part}" "${destination_path}/detached/${part}"
cp -a "${source_path}/${part}" "${destination_path}/detached/all_1000000_1000000_0"

# A pending replicated drop must win over a manually staged seed.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE preserve_safe_source DROP PART '${part}' SETTINGS alter_sync = 1"
expect_error BAD_ARGUMENTS "ALTER TABLE preserve_safe_destination ATTACH PART '${part}' SETTINGS attach_preserve_part_name = 1"
${CLICKHOUSE_CLIENT} --query "SELECT 'dropped_seed_preserved', count() = 1 FROM system.detached_parts WHERE database = currentDatabase() AND table = 'preserve_safe_destination' AND name = '${part}'"
expect_error BAD_ARGUMENTS "ALTER TABLE preserve_safe_destination ATTACH PART 'all_1000000_1000000_0' SETTINGS attach_preserve_part_name = 1"

# The attachment setting must not affect unrelated partition commands.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE preserve_safe_plain (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO preserve_safe_plain VALUES (1);
"
expect_error BAD_ARGUMENTS "ALTER TABLE preserve_safe_destination ATTACH PARTITION tuple() FROM preserve_safe_plain SETTINGS attach_preserve_part_name = 1"
${CLICKHOUSE_CLIENT} --multiquery --query "
    ALTER TABLE preserve_safe_plain DROP PARTITION tuple() SETTINGS attach_preserve_part_name = 1;
    SELECT 'unrelated_partition_command', count() FROM preserve_safe_plain;
"
