#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-object-storage
#
# `no-object-storage`: the test copies a part directory between two tables'
# on-disk `detached/` folders. On s3 / azure disks those files are
# `DiskObjectStorageMetadata` pointer files, not the actual data, so copying
# them between tables does not carry the data. Local disk is sufficient to
# exercise the level-reset logic which is disk-layer independent.
#
# Regression test for ClickHouse/ClickHouse#109674.
#
# A merged part from a plain MergeTree has level > 0 but may hold duplicate
# ORDER BY keys (plain MergeTree does not collapse rows on merge). When such a
# part is copied into a non-Ordinary engine's detached/ folder and ATTACHed,
# the old code kept the level clamped to 1. FINAL / OPTIMIZE and the
# PartsSplitter FINAL optimization treat a lone level > 0 part as already
# merged and skip the row-collapsing transform, so the duplicates survived
# SELECT ... FINAL (silent wrong results). The fix resets the adopted part's
# level to 0 on ATTACH PART/PARTITION so the destination re-merges it under
# its own semantics.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Build a foreign level-1 part with duplicate keys in a plain MergeTree, then
# attach it into each FINAL-relying engine and check FINAL deduplicates it.
run_case() {
    local engine="$1"
    local extra_cols="$2"      # extra column definitions for the dest table
    local src_extra_cols="$3"  # matching columns in the source MergeTree
    local src_values="$4"      # VALUES producing duplicate keys

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst SYNC"

    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE src (a UInt32 ${src_extra_cols}) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 8192"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO src VALUES ${src_values}"
    # Merge into a single level-1 part with duplicate keys (plain MergeTree keeps them).
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE src FINAL"

    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE dst (a UInt32 ${extra_cols}) ENGINE = ${engine} ORDER BY a
        SETTINGS index_granularity = 8192"

    local part
    part=$(${CLICKHOUSE_CLIENT} -q "
        SELECT name FROM system.parts
        WHERE database = currentDatabase() AND table = 'src' AND active LIMIT 1")

    local src_path dst_path
    src_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 'src'")
    dst_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 'dst'")

    # Sanity: the source part is a merged part (level > 0).
    echo "${engine} src part level > 0: $(echo "${part}" | awk -F_ '{print ($4 > 0) ? 1 : 0}')"

    mkdir -p "${dst_path}detached"
    cp -r "${src_path}${part}" "${dst_path}detached/${part}"

    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE dst ATTACH PART '${part}'"

    # Adopted part must have level 0 so FINAL re-merges it.
    echo "${engine} attached part level: $(${CLICKHOUSE_CLIENT} -q "
        SELECT max(level) FROM system.parts
        WHERE database = currentDatabase() AND table = 'dst' AND active")"

    # FINAL must deduplicate: 3 distinct keys expected.
    echo "${engine} FINAL count (expect 3): $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst FINAL")"
    # OPTIMIZE FINAL then a plain count must also give 3.
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE dst FINAL"
    echo "${engine} count after OPTIMIZE FINAL (expect 3): $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst")"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE src SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE dst SYNC"
}

# ReplacingMergeTree: 3 distinct keys, each duplicated once.
run_case "ReplacingMergeTree" ", b UInt32" ", b UInt32" "(1, 10), (1, 20), (2, 30), (2, 40), (3, 50), (3, 60)"
# SummingMergeTree: same key rows summed on merge.
run_case "SummingMergeTree" ", b UInt32" ", b UInt32" "(1, 10), (1, 20), (2, 30), (2, 40), (3, 50), (3, 60)"
# AggregatingMergeTree: with only ORDER BY key columns, same-key rows collapse.
run_case "AggregatingMergeTree" "" "" "(1), (1), (2), (2), (3), (3)"
