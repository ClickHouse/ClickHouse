#pragma once

namespace Iceberg
{
/// This file define the field name appearing in Iceberg files.
#define DEFINE_ICEBERG_FIELD_ALIAS(name, strval) constexpr const char * f_##name = #strval;
#define DEFINE_ICEBERG_FIELD_COMPOUND(name, subname) constexpr const char * c_##name##_##subname = #name "." #subname;
#define DEFINE_ICEBERG_FIELD(name) constexpr const char * f_##name = #name;

/// These variables begin with 'f_', following the field name in Iceberg files.
DEFINE_ICEBERG_FIELD(data_file);
DEFINE_ICEBERG_FIELD(location);
DEFINE_ICEBERG_FIELD(manifest_path);
DEFINE_ICEBERG_FIELD(schemas);
DEFINE_ICEBERG_FIELD(sequence_number);
DEFINE_ICEBERG_FIELD(snapshots);
DEFINE_ICEBERG_FIELD(status);
DEFINE_ICEBERG_FIELD(summary);
DEFINE_ICEBERG_FIELD(transform);
/// These variables replace `-` with underscore `_` to be compatible with c++ code.
DEFINE_ICEBERG_FIELD_ALIAS(format_version, format-version);
DEFINE_ICEBERG_FIELD_ALIAS(current_snapshot_id, current-snapshot-id);
DEFINE_ICEBERG_FIELD_ALIAS(snapshot_id, snapshot-id);
DEFINE_ICEBERG_FIELD_ALIAS(parent_snapshot_id, parent-snapshot-id);
DEFINE_ICEBERG_FIELD_ALIAS(snapshot_log, snapshot-log);
DEFINE_ICEBERG_FIELD_ALIAS(schema_id, schema-id);
DEFINE_ICEBERG_FIELD_ALIAS(current_schema_id, current-schema-id);
DEFINE_ICEBERG_FIELD_ALIAS(table_uuid, table-uuid);
DEFINE_ICEBERG_FIELD_ALIAS(total_records, total-records);
DEFINE_ICEBERG_FIELD_ALIAS(total_files_size, total-files-size);
DEFINE_ICEBERG_FIELD_ALIAS(manifest_list, manifest-list);
DEFINE_ICEBERG_FIELD_ALIAS(timestamp_ms, timestamp-ms);
DEFINE_ICEBERG_FIELD_ALIAS(last_updated_ms, last-updated-ms);
DEFINE_ICEBERG_FIELD_ALIAS(source_id, source-id);
/// These are compound fields like `data_file.file_path`, we use prefix 'c_' to distinguish them.
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, file_path);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, content);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, partition);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, value_counts);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, column_sizes);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, null_value_counts);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, lower_bounds);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, upper_bounds);
}
