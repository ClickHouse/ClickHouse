#!/usr/bin/env bash
# Tags: no-fasttest, no-random-merge-tree-settings
#
# `no-random-merge-tree-settings`: the test pins escape_index_filenames and
# packed_skip_index_max_bytes, which are exactly the settings the collision
# depends on.
#
# DROP INDEX must not delete files owned by a surviving index.
#
# The DROP INDEX bookkeeping has to enumerate substream suffixes speculatively
# (the dropped index's type is already gone from metadata by then, so its real
# substream list is unavailable). With escape_index_filenames = 0 the stream name
# is the index name verbatim, so "drop index a" + speculative suffix ".pos"
# addresses skp_idx_a.pos.*, which is the on-disk name of an unrelated surviving
# index literally named `a.pos`. Both the standalone and the packed-archive path
# must skip any candidate a surviving index owns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $1: label, $2: escape_index_filenames, $3: packed_skip_index_max_bytes
run_case() {
    local label="$1" escape="$2" packed="$3"
    local tbl="t_coll_${label}"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${tbl} SYNC"

    # v = k and w = k are monotone, so each minmax index prunes a point query to
    # a single granule. index_granularity = 100 over 500 rows gives 5 granules.
    ${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${tbl}
    (
        k UInt64,
        v UInt64,
        w UInt64,
        INDEX a v TYPE minmax GRANULARITY 1,
        INDEX \`a.pos\` w TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             escape_index_filenames = ${escape},
             packed_skip_index_max_bytes = ${packed}"

    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${tbl} (k, v, w) SELECT number, number, number FROM numbers(500)"
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${tbl} FINAL"

    echo "${label}_before_both_indices_prune:"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%'"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"

    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${tbl} DROP INDEX a SETTINGS mutations_sync = 2"

    # The dropped index is gone...
    echo "${label}_dropped_index_gone:"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a'"

    # ...and the sibling survived, with its files intact enough to still prune.
    echo "${label}_sibling_survives:"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a.pos'"
    echo "${label}_sibling_still_prunes:"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"

    echo "${label}_check_table:"
    ${CLICKHOUSE_CLIENT} -q "CHECK TABLE ${tbl}" | cut -f2

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE ${tbl} SYNC"
}

# Unescaped names are the shape that collides: the stream name is the index name
# verbatim, so `a` + ".pos" == the stream name of index `a.pos`.
run_case "standalone_unescaped" 0 0
# Escaping makes the sibling's file skp_idx_a%2Epos.*, so no collision is
# possible; kept as a control that the guard does not break the ordinary path.
run_case "standalone_escaped" 1 0
# Same collision, but the files live inside skp_idx.packed and are removed by the
# archive filter rather than by a rename-to-empty.
run_case "packed_unescaped" 0 1048576
