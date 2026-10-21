#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
# no-object-storage / no-shared-merge-tree: reads serialization.json from a local part directory.
#
# Flattenable Tuple leaves are gathered one at a time. Output files stay
# streams of the parent column. Vertical output is byte-compatible with Horizontal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

COMMON_SETTINGS="
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    replace_long_file_name_to_hash = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

print_t_substreams()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT arrayJoin(substreams)
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active AND column = 't'
        ORDER BY 1
    "
}

echo '=== all-Fat Vertical vs Horizontal ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_fat_v;
    DROP TABLE IF EXISTS t_fat_h;
    CREATE TABLE t_fat_v
    (
        k UInt64,
        t Tuple(x String, inner Tuple(c String, d String))
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    CREATE TABLE t_fat_h AS t_fat_v
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        enable_vertical_merge_algorithm = 0;

    INSERT INTO t_fat_v VALUES
        (1, ('', ('c1', ''))),
        (2, ('x2', ('', 'd2')));
    INSERT INTO t_fat_v VALUES
        (3, ('x3', ('c3', 'd3'))),
        (4, ('', ('c4', 'd4')));

    INSERT INTO t_fat_h VALUES
        (1, ('', ('c1', ''))),
        (2, ('x2', ('', 'd2')));
    INSERT INTO t_fat_h VALUES
        (3, ('x3', ('c3', 'd3'))),
        (4, ('', ('c4', 'd4')));

    OPTIMIZE TABLE t_fat_v FINAL;
    OPTIMIZE TABLE t_fat_h FINAL;
"

echo 'rows'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_fat_v"
echo 'select_match'
${CLICKHOUSE_CLIENT} -q "
    SELECT count() = 0 FROM
    (
        SELECT * FROM t_fat_v
        EXCEPT
        SELECT * FROM t_fat_h
    )
"
echo 'check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_fat_v SETTINGS check_query_single_value_result = 1"

echo 'vertical_substreams'
print_t_substreams t_fat_v
echo 'horizontal_substreams'
print_t_substreams t_fat_h

echo 'substreams_equal'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        (SELECT any(substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_fat_v' AND active AND column = 't')
        =
        (SELECT any(substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_fat_h' AND active AND column = 't')
"

echo 'serialization_keys'
V_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_fat_v' AND active")
python3 - "$V_PATH" <<'PY'
import json, os, sys
path = os.path.join(sys.argv[1], "serialization.json")
if not os.path.exists(path):
    print("missing")
    raise SystemExit(0)
with open(path) as f:
    data = json.load(f)
cols = data.get("columns", [])
print(*[c.get("name") for c in cols])
for col in cols:
    if col.get("name") == "t":
        print("t_exact", col.get("exact_num_defaults", False))
        for sub in col.get("subcolumns", []):
            if "subcolumns" in sub:
                print("inner_exact", sub.get("exact_num_defaults", False))
PY

echo 'merge_algorithm'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT merge_algorithm
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 't_fat_v' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_fat_v; DROP TABLE t_fat_h;"

echo
echo '=== JSON leaf does not flatten ==='

${CLICKHOUSE_CLIENT} -q "
    SET enable_json_type = 1;
    DROP TABLE IF EXISTS t_json;
    CREATE TABLE t_json
    (
        k UInt64,
        t Tuple(x String, j JSON)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_json VALUES (1, ('a', '{\"p\":1}'));
    INSERT INTO t_json VALUES (2, ('b', '{\"p\":2}'));
    OPTIMIZE TABLE t_json FINAL;
    SELECT k, t.x, t.j.p FROM t_json ORDER BY k;
    CHECK TABLE t_json SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_json;
"

echo
echo '=== Compact source does not flatten ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_compact;
    CREATE TABLE t_compact
    (
        k UInt64,
        t Tuple(x String, y String)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        min_rows_for_wide_part = 100000,
        min_bytes_for_wide_part = 100000000,
        allow_vertical_merges_from_compact_to_wide_parts = 1;

    INSERT INTO t_compact VALUES (1, ('a', 'b'));
    INSERT INTO t_compact VALUES (2, ('c', 'd'));
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active ORDER BY name;
    ALTER TABLE t_compact MODIFY SETTING min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
    OPTIMIZE TABLE t_compact FINAL;
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
    SELECT k, t FROM t_compact ORDER BY k;
    CHECK TABLE t_compact SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_compact;
"

echo
echo '=== ADD tuple element: old Wide part lacks the leaf ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_add;
    CREATE TABLE t_add
    (
        k UInt64,
        t Tuple(x String, y String)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_add VALUES (1, ('a', 'b'));
    INSERT INTO t_add VALUES (2, ('c', 'd'));
    ALTER TABLE t_add MODIFY COLUMN t Tuple(x String, y String, z String);
    INSERT INTO t_add VALUES (3, ('e', 'f', 'g'));
    OPTIMIZE TABLE t_add FINAL;
    SELECT k, t FROM t_add ORDER BY k;
    CHECK TABLE t_add SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_add;
"
