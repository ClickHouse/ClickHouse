#pragma once

namespace Iceberg
{
#define DEFINE_ICEBERG_FIELD_NAME(name, strval) constexpr const char * F##name = #strval;
#define DEFINE_ICEBERG_SUB_FIELD_NAME(name, subname) constexpr const char * F##name##0##subname = #name "." #subname;
#define DEFINE_ICEBERG_FIELD_NAME_SAME(name) constexpr const char * F##name = #name;

DEFINE_ICEBERG_FIELD_NAME_SAME(sequence_number);
DEFINE_ICEBERG_FIELD_NAME_SAME(manifest_path);
DEFINE_ICEBERG_FIELD_NAME(format_version, format-version);
DEFINE_ICEBERG_FIELD_NAME(current_snapshot_id, current-snapshot-id);
DEFINE_ICEBERG_FIELD_NAME(snapshot_id, snapshot-id);
DEFINE_ICEBERG_FIELD_NAME(schema_id, schema-id);
DEFINE_ICEBERG_FIELD_NAME(current_schema_id, current-schema-id);
DEFINE_ICEBERG_FIELD_NAME(table_uuid, table-uuid);
DEFINE_ICEBERG_FIELD_NAME(total_records, total-records);
DEFINE_ICEBERG_FIELD_NAME(total_files_size, total-files-size);
DEFINE_ICEBERG_FIELD_NAME(manifest_list, manifest-list);
DEFINE_ICEBERG_FIELD_NAME(snapshot_log, snapshot-log);
DEFINE_ICEBERG_FIELD_NAME(timestamp_ms, timestamp-ms);
DEFINE_ICEBERG_FIELD_NAME_SAME(location);
DEFINE_ICEBERG_FIELD_NAME_SAME(snapshots);
DEFINE_ICEBERG_FIELD_NAME_SAME(schemas);
DEFINE_ICEBERG_FIELD_NAME(last_updated_ms, last-updated-ms);
DEFINE_ICEBERG_FIELD_NAME(source_id, source-id);
DEFINE_ICEBERG_FIELD_NAME_SAME(transform);
DEFINE_ICEBERG_FIELD_NAME_SAME(status);
DEFINE_ICEBERG_FIELD_NAME_SAME(data_file);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, file_path);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, content);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, partition);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, value_counts);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, column_sizes);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, null_value_counts);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, lower_bounds);
DEFINE_ICEBERG_SUB_FIELD_NAME(data_file, upper_bounds);
}
