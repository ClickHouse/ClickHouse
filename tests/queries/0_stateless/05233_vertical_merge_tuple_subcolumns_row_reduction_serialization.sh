#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
# no-object-storage / no-shared-merge-tree: reads serialization.json from a local part directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_tuple_row_reduction;
        DROP TABLE IF EXISTS t_tuple_all_non_sparse;
    " 2>/dev/null || true
}
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_tuple_row_reduction
    (
        k UInt64,
        t Tuple(a Array(UInt8), s String),
        sign Int8
    )
    ENGINE = CollapsingMergeTree(sign)
    ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        ratio_of_defaults_for_sparse_serialization = 0.9,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        auto_statistics_types = '';

    SYSTEM STOP MERGES t_tuple_row_reduction;
    INSERT INTO t_tuple_row_reduction VALUES
        (1, ([1], 'drop'), 1),
        (2, ([2], 'keep'), 1);
    INSERT INTO t_tuple_row_reduction VALUES
        (1, ([1], 'drop'), -1),
        (3, ([3], 'keep'), 1);
    SYSTEM START MERGES t_tuple_row_reduction;
    OPTIMIZE TABLE t_tuple_row_reduction FINAL;
"

echo 'rows'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_tuple_row_reduction"

echo 'serialization_rows'
PART_PATH=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_tuple_row_reduction' AND active
")
python3 - "$PART_PATH" <<'PY'
import json
import os
import sys

with open(os.path.join(sys.argv[1], "serialization.json")) as serialization_file:
    serialization = json.load(serialization_file)

tuple_info = next(column for column in serialization["columns"] if column["name"] == "t")
rows = [tuple_info["num_rows"], *(subcolumn["num_rows"] for subcolumn in tuple_info["subcolumns"])]
print(*rows)
if rows != [2, 2, 2]:
    raise SystemExit(f"Unexpected serialization row counts: {rows}")
PY

echo 'merge_algorithm'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT merge_algorithm
    FROM system.part_log
    WHERE database = currentDatabase()
      AND table = 't_tuple_row_reduction'
      AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_tuple_all_non_sparse
    (
        k UInt64,
        t Tuple(a Array(UInt8), b Array(UInt8)),
        sign Int8
    )
    ENGINE = CollapsingMergeTree(sign)
    ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        ratio_of_defaults_for_sparse_serialization = 0.9,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        auto_statistics_types = '';

    SYSTEM STOP MERGES t_tuple_all_non_sparse;
    INSERT INTO t_tuple_all_non_sparse VALUES
        (1, ([1], [1]), 1),
        (2, ([2], [2]), 1);
    INSERT INTO t_tuple_all_non_sparse VALUES
        (1, ([1], [1]), -1),
        (3, ([3], [3]), 1);
    SYSTEM START MERGES t_tuple_all_non_sparse;
    OPTIMIZE TABLE t_tuple_all_non_sparse FINAL;
"

echo 'all_non_sparse_serialization_rows'
PART_PATH=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_tuple_all_non_sparse' AND active
")
python3 - "$PART_PATH" <<'PY'
import json
import os
import sys

with open(os.path.join(sys.argv[1], "serialization.json")) as serialization_file:
    serialization = json.load(serialization_file)

tuple_info = next(column for column in serialization["columns"] if column["name"] == "t")
rows = [tuple_info["num_rows"], *(subcolumn["num_rows"] for subcolumn in tuple_info["subcolumns"])]
print(*rows)
if rows != [2, 2, 2]:
    raise SystemExit(f"Unexpected serialization row counts: {rows}")
PY

echo 'all_non_sparse_merge_algorithm'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT merge_algorithm
    FROM system.part_log
    WHERE database = currentDatabase()
      AND table = 't_tuple_all_non_sparse'
      AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
"
