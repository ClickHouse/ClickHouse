#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

namespace DB::IcebergRESTCatalog
{

/// Names that appear in Iceberg REST catalog requests. Names of table metadata fields are in `Iceberg/Constant.h`.

/// Requirement types of a commit request.
DEFINE_ICEBERG_FIELD_ALIAS(assert_create, assert-create);
DEFINE_ICEBERG_FIELD_ALIAS(assert_table_uuid, assert-table-uuid);
DEFINE_ICEBERG_FIELD_ALIAS(assert_ref_snapshot_id, assert-ref-snapshot-id);
DEFINE_ICEBERG_FIELD_ALIAS(assert_current_schema_id, assert-current-schema-id);
DEFINE_ICEBERG_FIELD_ALIAS(assert_last_assigned_field_id, assert-last-assigned-field-id);
DEFINE_ICEBERG_FIELD_ALIAS(assert_last_assigned_partition_id, assert-last-assigned-partition-id);
DEFINE_ICEBERG_FIELD_ALIAS(assert_default_spec_id, assert-default-spec-id);
DEFINE_ICEBERG_FIELD_ALIAS(assert_default_sort_order_id, assert-default-sort-order-id);

/// Update actions of a commit request.
DEFINE_ICEBERG_FIELD_ALIAS(add_snapshot, add-snapshot);
DEFINE_ICEBERG_FIELD_ALIAS(set_snapshot_ref, set-snapshot-ref);
DEFINE_ICEBERG_FIELD_ALIAS(remove_snapshot_ref, remove-snapshot-ref);
DEFINE_ICEBERG_FIELD_ALIAS(set_properties, set-properties);
DEFINE_ICEBERG_FIELD_ALIAS(remove_properties, remove-properties);
DEFINE_ICEBERG_FIELD_ALIAS(assign_uuid, assign-uuid);
DEFINE_ICEBERG_FIELD_ALIAS(upgrade_format_version, upgrade-format-version);
DEFINE_ICEBERG_FIELD_ALIAS(add_schema, add-schema);
DEFINE_ICEBERG_FIELD_ALIAS(set_current_schema, set-current-schema);
DEFINE_ICEBERG_FIELD_ALIAS(add_spec, add-spec);
DEFINE_ICEBERG_FIELD_ALIAS(set_default_spec, set-default-spec);
DEFINE_ICEBERG_FIELD_ALIAS(add_sort_order, add-sort-order);
DEFINE_ICEBERG_FIELD_ALIAS(set_default_sort_order, set-default-sort-order);
DEFINE_ICEBERG_FIELD_ALIAS(remove_snapshots, remove-snapshots);
DEFINE_ICEBERG_FIELD_ALIAS(set_location, set-location);
DEFINE_ICEBERG_FIELD_ALIAS(set_statistics, set-statistics);
DEFINE_ICEBERG_FIELD_ALIAS(remove_statistics, remove-statistics);
DEFINE_ICEBERG_FIELD_ALIAS(set_partition_statistics, set-partition-statistics);
DEFINE_ICEBERG_FIELD_ALIAS(remove_partition_statistics, remove-partition-statistics);
DEFINE_ICEBERG_FIELD_ALIAS(remove_partition_specs, remove-partition-specs);
DEFINE_ICEBERG_FIELD_ALIAS(remove_schemas, remove-schemas);
DEFINE_ICEBERG_FIELD_ALIAS(add_encryption_key, add-encryption-key);
DEFINE_ICEBERG_FIELD_ALIAS(remove_encryption_key, remove-encryption-key);

/// Fields of requirements and updates.
DEFINE_ICEBERG_FIELD(action);
DEFINE_ICEBERG_FIELD(ref);
DEFINE_ICEBERG_FIELD_ALIAS(ref_name, ref-name);
DEFINE_ICEBERG_FIELD(snapshot);
DEFINE_ICEBERG_FIELD_ALIAS(snapshot_ids, snapshot-ids);
DEFINE_ICEBERG_FIELD(updates);
DEFINE_ICEBERG_FIELD(removals);
DEFINE_ICEBERG_FIELD(spec);
DEFINE_ICEBERG_FIELD_ALIAS(sort_order, sort-order);
DEFINE_ICEBERG_FIELD_ALIAS(update_sort_order_id, sort-order-id);
DEFINE_ICEBERG_FIELD_ALIAS(last_assigned_field_id, last-assigned-field-id);
DEFINE_ICEBERG_FIELD_ALIAS(last_assigned_partition_id, last-assigned-partition-id);

}
