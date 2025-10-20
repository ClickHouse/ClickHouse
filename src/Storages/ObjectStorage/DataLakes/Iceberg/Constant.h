#pragma once

namespace DB::Iceberg
{
/// This file define the field name appearing in Iceberg files.
#define DEFINE_ICEBERG_FIELD_ALIAS(name, strval) constexpr const char * f_##name = #strval;
#define DEFINE_ICEBERG_FIELD_COMPOUND(name, subname) constexpr const char * c_##name##_##subname = #name "." #subname;
#define DEFINE_ICEBERG_FIELD(name) constexpr const char * f_##name = #name;

/// These variables begin with 'f_', following the field name in Iceberg files.
DEFINE_ICEBERG_FIELD(boolean);
DEFINE_ICEBERG_FIELD(bigint);
DEFINE_ICEBERG_FIELD(binary);
DEFINE_ICEBERG_FIELD(double);
DEFINE_ICEBERG_FIELD(date);
DEFINE_ICEBERG_FIELD(data_file);
DEFINE_ICEBERG_FIELD(element);
DEFINE_ICEBERG_FIELD(fields);
DEFINE_ICEBERG_FIELD(float);
DEFINE_ICEBERG_FIELD(key);
DEFINE_ICEBERG_FIELD(list)
DEFINE_ICEBERG_FIELD(location);
DEFINE_ICEBERG_FIELD(long);
DEFINE_ICEBERG_FIELD(id)
DEFINE_ICEBERG_FIELD(int)
DEFINE_ICEBERG_FIELD(manifest_path);
DEFINE_ICEBERG_FIELD(map);
DEFINE_ICEBERG_FIELD(name);
DEFINE_ICEBERG_FIELD(required);
DEFINE_ICEBERG_FIELD(schema);
DEFINE_ICEBERG_FIELD(schemas);
DEFINE_ICEBERG_FIELD(sequence_number);
DEFINE_ICEBERG_FIELD(snapshots);
DEFINE_ICEBERG_FIELD(status);
DEFINE_ICEBERG_FIELD(struct);
DEFINE_ICEBERG_FIELD(string);
DEFINE_ICEBERG_FIELD(summary);
DEFINE_ICEBERG_FIELD(time);
DEFINE_ICEBERG_FIELD(timestamp);
DEFINE_ICEBERG_FIELD(timestamptz);
DEFINE_ICEBERG_FIELD(type)
DEFINE_ICEBERG_FIELD(transform);
DEFINE_ICEBERG_FIELD(direction);

DEFINE_ICEBERG_FIELD(uuid);
DEFINE_ICEBERG_FIELD(value);
DEFINE_ICEBERG_FIELD(manifest_length);
DEFINE_ICEBERG_FIELD(partition_spec_id);
DEFINE_ICEBERG_FIELD(content);
DEFINE_ICEBERG_FIELD(min_sequence_number);
DEFINE_ICEBERG_FIELD(added_snapshot_id);
DEFINE_ICEBERG_FIELD(added_data_files_count);
DEFINE_ICEBERG_FIELD(existing_data_files_count);
DEFINE_ICEBERG_FIELD(deleted_data_files_count);
DEFINE_ICEBERG_FIELD(added_files_count);
DEFINE_ICEBERG_FIELD(existing_files_count);
DEFINE_ICEBERG_FIELD(deleted_files_count);
DEFINE_ICEBERG_FIELD(added_rows_count);
DEFINE_ICEBERG_FIELD(existing_rows_count);
DEFINE_ICEBERG_FIELD(deleted_rows_count);
DEFINE_ICEBERG_FIELD(record_count);
DEFINE_ICEBERG_FIELD(file_path);
DEFINE_ICEBERG_FIELD(file_format);
DEFINE_ICEBERG_FIELD(file_size_in_bytes);
DEFINE_ICEBERG_FIELD(refs);
DEFINE_ICEBERG_FIELD(branch);
DEFINE_ICEBERG_FIELD(main);
DEFINE_ICEBERG_FIELD(operation);
DEFINE_ICEBERG_FIELD(append);
DEFINE_ICEBERG_FIELD(overwrite);
DEFINE_ICEBERG_FIELD(file_sequence_number);
DEFINE_ICEBERG_FIELD(snapshot_id);
DEFINE_ICEBERG_FIELD(statistics);
DEFINE_ICEBERG_FIELD(properties);
DEFINE_ICEBERG_FIELD(owner);
DEFINE_ICEBERG_FIELD(column_sizes);
DEFINE_ICEBERG_FIELD(null_value_counts);
DEFINE_ICEBERG_FIELD(lower_bounds);
DEFINE_ICEBERG_FIELD(upper_bounds);


/// These variables replace `-` with underscore `_` to be compatible with c++ code.
DEFINE_ICEBERG_FIELD_ALIAS(format_version, format-version);
DEFINE_ICEBERG_FIELD_ALIAS(current_snapshot_id, current-snapshot-id);
DEFINE_ICEBERG_FIELD_ALIAS(metadata_snapshot_id, snapshot-id);
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
DEFINE_ICEBERG_FIELD_ALIAS(last_column_id, last-column-id);
DEFINE_ICEBERG_FIELD_ALIAS(element_id, element-id);
DEFINE_ICEBERG_FIELD_ALIAS(element_required, element-required);
DEFINE_ICEBERG_FIELD_ALIAS(key_id, key-id);
DEFINE_ICEBERG_FIELD_ALIAS(value_id, value-id);
DEFINE_ICEBERG_FIELD_ALIAS(value_required, value-required);
DEFINE_ICEBERG_FIELD_ALIAS(last_partition_id, last-partition-id);
DEFINE_ICEBERG_FIELD_ALIAS(order_id, order-id);
DEFINE_ICEBERG_FIELD_ALIAS(default_sort_order_id, default-sort-order-id);
DEFINE_ICEBERG_FIELD_ALIAS(sort_orders, sort-orders);
DEFINE_ICEBERG_FIELD_ALIAS(source_id, source-id);
DEFINE_ICEBERG_FIELD_ALIAS(partition_transform, transform);
DEFINE_ICEBERG_FIELD_ALIAS(partition_name, name);
DEFINE_ICEBERG_FIELD_ALIAS(default_spec_id, default-spec-id);
DEFINE_ICEBERG_FIELD_ALIAS(partition_spec, partition-spec);
DEFINE_ICEBERG_FIELD_ALIAS(partition_specs, partition-specs);
DEFINE_ICEBERG_FIELD_ALIAS(spec_id, spec-id);
DEFINE_ICEBERG_FIELD_ALIAS(added_records, added-records);
DEFINE_ICEBERG_FIELD_ALIAS(added_data_files, added-data-files);
DEFINE_ICEBERG_FIELD_ALIAS(added_delete_files, added-delete-files);
DEFINE_ICEBERG_FIELD_ALIAS(added_position_delete_files, added-position-delete-files);
DEFINE_ICEBERG_FIELD_ALIAS(added_position_deletes, added-position-deletes);
DEFINE_ICEBERG_FIELD_ALIAS(added_files_size, added-files-size);
DEFINE_ICEBERG_FIELD_ALIAS(total_data_files, total-data-files);
DEFINE_ICEBERG_FIELD_ALIAS(changed_partition_count, changed-partition-count);
DEFINE_ICEBERG_FIELD_ALIAS(total_delete_files, total-delete-files);
DEFINE_ICEBERG_FIELD_ALIAS(total_position_deletes, total-position-deletes);
DEFINE_ICEBERG_FIELD_ALIAS(total_equality_deletes, total-equality-deletes);
DEFINE_ICEBERG_FIELD_ALIAS(field_id, field-id);
DEFINE_ICEBERG_FIELD_ALIAS(last_sequence_number, last-sequence-number);
DEFINE_ICEBERG_FIELD_ALIAS(metadata_file, metadata-file);
DEFINE_ICEBERG_FIELD_ALIAS(metadata_log, metadata-log);
DEFINE_ICEBERG_FIELD_ALIAS(metadata_sequence_number, sequence-number);
/// These are compound fields like `data_file.file_path`, we use prefix 'c_' to distinguish them.
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, file_path);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, file_format);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, content);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, equality_ids);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, partition);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, value_counts);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, column_sizes);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, null_value_counts);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, lower_bounds);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, upper_bounds);
DEFINE_ICEBERG_FIELD_COMPOUND(data_file, referenced_data_file);
}
