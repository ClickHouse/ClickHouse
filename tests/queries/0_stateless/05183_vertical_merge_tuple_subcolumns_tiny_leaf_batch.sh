#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
# no-object-storage / no-shared-merge-tree: reads part files from a local directory.
#
# Step 3: FatLeaf + TinyLeafBatch. Default 10 MiB Fat threshold, fat `String` plus
# small siblings gathered in one batch. Output files stay streams of parent `t`.

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
    allow_experimental_vertical_merge_tuple_subcolumns = 1
"

print_part_files()
{
    local path="$1"
    find "$path" -maxdepth 1 -type f -printf '%f\n' | grep -E '^t' | sort
}

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

echo '=== Fat String + tiny siblings, default 10 MiB ==='

${CLICKHOUSE_CLIENT} -q "
    SET allow_suspicious_low_cardinality_types = 1;
    DROP TABLE IF EXISTS t_tiny_v;
    DROP TABLE IF EXISTS t_tiny_h;
    CREATE TABLE t_tiny_v
    (
        k UInt64,
        t Tuple(
            s String,
            n UInt8,
            nnull Nullable(UInt8),
            lc LowCardinality(UInt8),
            tiny String)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    CREATE TABLE t_tiny_h AS t_tiny_v
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        enable_vertical_merge_algorithm = 0;

    INSERT INTO t_tiny_v SELECT
        number,
        (repeat('x', 12000), toUInt8(number), toUInt8(number % 3), toUInt8(number % 5), 'ab')
    FROM numbers(1000);
    INSERT INTO t_tiny_v SELECT
        number + 1000,
        (repeat('y', 12000), toUInt8(number), toUInt8(number % 3), toUInt8(number % 5), 'cd')
    FROM numbers(1000);

    INSERT INTO t_tiny_h SELECT
        number,
        (repeat('x', 12000), toUInt8(number), toUInt8(number % 3), toUInt8(number % 5), 'ab')
    FROM numbers(1000);
    INSERT INTO t_tiny_h SELECT
        number + 1000,
        (repeat('y', 12000), toUInt8(number), toUInt8(number % 3), toUInt8(number % 5), 'cd')
    FROM numbers(1000);

    OPTIMIZE TABLE t_tiny_v FINAL;
    OPTIMIZE TABLE t_tiny_h FINAL;
"

echo 'rows'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_tiny_v"
echo 'select_match'
${CLICKHOUSE_CLIENT} -q "
    SELECT count() = 0 FROM
    (
        SELECT * FROM t_tiny_v
        EXCEPT
        SELECT * FROM t_tiny_h
    )
"
echo 'leaf_sums'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        sum(t.n),
        countIf(t.nnull IS NULL),
        sum(t.lc),
        min(length(t.s)),
        max(length(t.s)),
        min(t.tiny),
        max(t.tiny)
    FROM t_tiny_v
"
echo 'check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_tiny_v SETTINGS check_query_single_value_result = 1"

echo 'vertical_files'
print_part_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_tiny_v' AND active")"
echo 'horizontal_files'
print_part_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_tiny_h' AND active")"

echo 'substreams_equal'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        (SELECT any(substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tiny_v' AND active AND column = 't')
        =
        (SELECT any(substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tiny_h' AND active AND column = 't')
"

echo 'serialization_keys'
V_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_tiny_v' AND active")
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
PY

echo 'merge_algorithm'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT merge_algorithm
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 't_tiny_v' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_tiny_v; DROP TABLE t_tiny_h;"

echo
echo '=== skewed fat String is FatLeaf, not Tiny ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_skew;
    CREATE TABLE t_skew
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        index_granularity = 8192;

    INSERT INTO t_skew
    SELECT 0, (arrayStringConcat(arrayMap(i -> repeat('x', 1000000), range(12))), toUInt8(0))
    UNION ALL
    SELECT number + 1, ('y', toUInt8(number + 1)) FROM numbers(9999);

    INSERT INTO t_skew
    SELECT 10000, (arrayStringConcat(arrayMap(i -> repeat('z', 1000000), range(12))), toUInt8(0))
    UNION ALL
    SELECT number + 10001, ('w', toUInt8(number + 1)) FROM numbers(9999);
    OPTIMIZE TABLE t_skew FINAL;
"

echo 'skew_rows'
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(t.n), max(length(t.s)), min(length(t.s)) FROM t_skew"
echo 'skew_check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_skew SETTINGS check_query_single_value_result = 1"
echo 'skew_files'
print_part_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_skew' AND active")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_skew;"

echo
echo '=== skip index on tiny leaf t.n ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_idx_tiny;
    CREATE TABLE t_idx_tiny
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        INDEX idx t.n TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        index_granularity = 1;

    INSERT INTO t_idx_tiny SELECT number, (repeat('x', 12000), toUInt8(number % 200)) FROM numbers(1000);
    INSERT INTO t_idx_tiny SELECT number + 1000, (repeat('y', 12000), toUInt8(number % 200)) FROM numbers(1000);
    OPTIMIZE TABLE t_idx_tiny FINAL;
    SELECT count() FROM t_idx_tiny WHERE t.n = 3 SETTINGS force_data_skipping_indices = 'idx';
    CHECK TABLE t_idx_tiny SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_idx_tiny;
"
