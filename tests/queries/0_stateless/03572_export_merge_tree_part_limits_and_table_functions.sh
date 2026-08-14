#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires s3 storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

big_table="big_table_${RANDOM}"
big_destination_max_bytes="big_destination_max_bytes_${RANDOM}"
big_destination_max_rows="big_destination_max_rows_${RANDOM}"
tf_schema_inherit="tf_schema_inherit_${RANDOM}"
tf_schema_explicit="tf_schema_explicit_${RANDOM}"
mt_table_tf="mt_table_tf_${RANDOM}"

query() {
    local query_text="$1"
    local query_id="$2"
    
    if [ -n "$query_id" ]; then
        $CLICKHOUSE_CLIENT --query_id="$query_id" --query "$query_text"
    else
        $CLICKHOUSE_CLIENT --query "$query_text"
    fi
}

query "DROP TABLE IF EXISTS $big_table, $big_destination_max_bytes, $big_destination_max_rows, $mt_table_tf"

echo "---- Test max_bytes and max_rows per file"

# Create all tables
query "CREATE TABLE $big_table (id UInt64, data String, year UInt16) Engine=MergeTree() order by id partition by year"
query "CREATE TABLE $big_destination_max_bytes(id UInt64, data String, year UInt16) engine=S3(s3_conn, filename='$big_destination_max_bytes', partition_strategy='hive', format=Parquet) partition by year"
query "CREATE TABLE $big_destination_max_rows(id UInt64, data String, year UInt16) engine=S3(s3_conn, filename='$big_destination_max_rows', partition_strategy='hive', format=Parquet) partition by year"
query "CREATE TABLE $mt_table_tf (id UInt64, value String, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()"

# Insert all data
# 4194304 is a number that came up during multiple iterations, it does not really mean anything (aside from the fact that the below numbers depend on it)
query "INSERT INTO $big_table SELECT number AS id, repeat('x', 100) AS data, 2025 AS year FROM numbers(4194304)"
query "INSERT INTO $big_table SELECT number AS id, repeat('x', 100) AS data, 2026 AS year FROM numbers(4194304)"
query "INSERT INTO $mt_table_tf VALUES (100, 'test1', 2022), (101, 'test2', 2022), (102, 'test3', 2023)"

# make sure we have only one part
query "OPTIMIZE TABLE $big_table FINAL"

# Get part names
big_part_max_bytes=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$big_table' AND partition_id = '2025' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
big_part_max_rows=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$big_table' AND partition_id = '2026' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')

# ============================================================================
# ALL EXPORTS HAPPEN HERE
# ============================================================================

# Generate unique query_ids for each export to track them in part_log
export_query_id_1="export_${RANDOM}_1"
export_query_id_2="export_${RANDOM}_2"
export_query_id_3="export_${RANDOM}_3"
export_query_id_4="export_${RANDOM}_4"

# this should generate ~4 files
query "ALTER TABLE $big_table EXPORT PART '$big_part_max_bytes' TO TABLE $big_destination_max_bytes SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_max_bytes_per_file=3500000, output_format_parquet_row_group_size_bytes=1000000" "$export_query_id_1"
# export_merge_tree_part_max_rows_per_file = 1048576 (which is 4194304/4) to generate 4 files
query "ALTER TABLE $big_table EXPORT PART '$big_part_max_rows' TO TABLE $big_destination_max_rows SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_max_rows_per_file=1048576" "$export_query_id_2"

echo "---- Table function with schema inheritance (no schema specified)"
query "ALTER TABLE $mt_table_tf EXPORT PART '2022_1_1_0' TO TABLE FUNCTION s3(s3_conn, filename='$tf_schema_inherit', format='Parquet', partition_strategy='hive') PARTITION BY year SETTINGS allow_experimental_export_merge_tree_part = 1" "$export_query_id_3"

echo "---- Table function with explicit compatible schema"
query "ALTER TABLE $mt_table_tf EXPORT PART '2023_2_2_0' TO TABLE FUNCTION s3(s3_conn, filename='$tf_schema_explicit', format='Parquet', structure='id UInt64, value String, year UInt16', partition_strategy='hive') PARTITION BY year SETTINGS allow_experimental_export_merge_tree_part = 1" "$export_query_id_4"

# Wait for all exports to complete
wait_for_exports() {
    local timeout=${1:-60}
    local poll_interval=${2:-0.5}
    local start_time=$(date +%s)
    local elapsed=0
    
    echo "Waiting for exports to complete (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        # Flush logs to ensure part_log entries are visible
        query "SYSTEM FLUSH LOGS" > /dev/null 2>&1 || true

        # Wait for part_log entries - these are written synchronously when export completes
        # Check if all expected exports have corresponding part_log entries by query_id
        local completed_count=$(query "SELECT count() FROM system.part_log WHERE event_type = 'ExportPart' AND query_id IN ('$export_query_id_1', '$export_query_id_2', '$export_query_id_3', '$export_query_id_4')" | tr -d '\n')
        
        if [ "$completed_count" = "4" ]; then
            echo "All exports completed."
            return 0
        fi
        
        sleep $poll_interval
        elapsed=$(($(date +%s) - start_time))
    done
    
    echo "Timeout waiting for exports to complete after ${timeout}s"
    query "SYSTEM FLUSH LOGS" > /dev/null 2>&1 || true
    echo "Completed exports in part_log:"
    query "SELECT query_id, table, part_name, event_time FROM system.part_log WHERE event_type = 'ExportPart' AND query_id IN ('$export_query_id_1', '$export_query_id_2', '$export_query_id_3', '$export_query_id_4')"
    echo "Remaining exports in system.exports:"
    query "SELECT source_table, part_name, elapsed, rows_read, total_rows_to_read FROM system.exports WHERE ((source_table = '$big_table' AND part_name IN ('$big_part_max_bytes', '$big_part_max_rows')) OR (source_table = '$mt_table_tf' AND part_name IN ('2022_1_1_0', '2023_2_2_0')))"
    return 1
}

wait_for_exports 60

# ============================================================================
# ALL SELECTS/VERIFICATIONS HAPPEN HERE
# ============================================================================

echo "---- Count files in big_destination_max_bytes, should be 5 (4 parquet, 1 commit)"
query "SELECT count(_file) FROM s3(s3_conn, filename='$big_destination_max_bytes/**', format='One')"

echo "---- Count rows in big_table and big_destination_max_bytes"
query "SELECT COUNT() from $big_table WHERE year = 2025"
query "SELECT COUNT() from $big_destination_max_bytes"

echo "---- Count files in big_destination_max_rows, should be 5 (4 parquet, 1 commit)"
query "SELECT count(_file) FROM s3(s3_conn, filename='$big_destination_max_rows/**', format='One')"

echo "---- Count rows in big_table and big_destination_max_rows"
query "SELECT COUNT() from $big_table WHERE year = 2026"
query "SELECT COUNT() from $big_destination_max_rows"

echo "---- Data should be exported with inherited schema"
query "SELECT * FROM s3(s3_conn, filename='$tf_schema_inherit/**.parquet') ORDER BY id"

echo "---- Data should be exported with explicit schema"
query "SELECT * FROM s3(s3_conn, filename='$tf_schema_explicit/**.parquet') ORDER BY id"

query "DROP TABLE IF EXISTS $big_table, $big_destination_max_bytes, $big_destination_max_rows, $mt_table_tf"
