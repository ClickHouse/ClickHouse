#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-parallel-replicas
#
# A dotted Tuple element can have the same flattened name as a nested element
# or a synthetic subcolumn of another leaf. Such names do not identify one
# leaf, so they must not activate leaf-by-leaf Vertical merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_ambiguous_nested;
        DROP TABLE IF EXISTS t_ambiguous_synthetic;
    " 2>/dev/null || true
}
trap cleanup EXIT

COMMON_SETTINGS="
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 2,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

print_merge_algorithm()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT merge_algorithm
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = '${table}'
          AND event_type = 'MergeParts'
          AND length(merged_from) = 2
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    "
}

cleanup

echo '=== duplicate flattened leaf path ==='

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ambiguous_nested
    (
        k UInt8,
        t Tuple(\`a.b\` String, a Tuple(b String))
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    SYSTEM STOP MERGES t_ambiguous_nested;
    INSERT INTO t_ambiguous_nested VALUES (1, ('x', ('y')));
    INSERT INTO t_ambiguous_nested VALUES (2, ('u', ('v')));
"

${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 't_ambiguous_nested'
      AND active
"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM START MERGES t_ambiguous_nested;
    OPTIMIZE TABLE t_ambiguous_nested FINAL;
"
print_merge_algorithm t_ambiguous_nested

echo
echo '=== leaf shadows a synthetic subcolumn ==='

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ambiguous_synthetic
    (
        k UInt8,
        t Tuple(\`a.size\` UInt64, a String)
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    SYSTEM STOP MERGES t_ambiguous_synthetic;
    INSERT INTO t_ambiguous_synthetic VALUES (1, (11, 'x'));
    INSERT INTO t_ambiguous_synthetic VALUES (2, (22, 'yy'));
"

${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 't_ambiguous_synthetic'
      AND active
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_ambiguous_synthetic"

set +e
optimize_error=$(${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_ambiguous_synthetic FINAL" 2>&1)
set -e

echo 'optimize_failed'
if echo "${optimize_error}" | grep -q 'CANNOT_READ_ALL_DATA'
then
    echo 1
else
    echo 0
fi

print_merge_algorithm t_ambiguous_synthetic
