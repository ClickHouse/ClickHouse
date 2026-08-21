#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#include <climits>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

#include <Common/randomSeed.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::Setting
{
extern const SettingsBool allow_experimental_geo_types_in_iceberg;
}


namespace DB
{

namespace
{

Poco::JSON::Object::Ptr deepCopy(Poco::JSON::Object::Ptr obj)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj->stringify(oss);

    Poco::JSON::Parser parser;
    auto result = parser.parse(oss.str());
    return result.extract<Poco::JSON::Object::Ptr>();
}

bool checkValidSchemaEvolution(Poco::Dynamic::Var old_type, Poco::Dynamic::Var new_type)
{
    if (old_type.isString() && new_type.isString() && old_type.extract<String>() == new_type.extract<String>())
        return true;

    if (new_type.isString() && new_type.extract<String>() == "long" &&
        old_type.isString() && (old_type.extract<String>() == "long" ||  old_type.extract<String>() == "int"))
    {
        return true;
    }

    if (new_type.isString() && new_type.extract<String>() == "double" &&
        old_type.isString() && (old_type.extract<String>() == "float" ||  old_type.extract<String>() == "double"))
    {
        return true;
    }

    if (old_type.isString() && new_type.isString())
    {
        auto old_str = old_type.extract<String>();
        auto new_str = new_type.extract<String>();
        if (old_str.starts_with("decimal(") && old_str.ends_with(')')
            && new_str.starts_with("decimal(") && new_str.ends_with(')'))
        {
            auto parse = [](const String & s) -> std::pair<size_t, size_t>
            {
                DB::ReadBufferFromString buf(std::string_view(s.begin() + 8, s.end() - 1));
                size_t p = 0, sc = 0;
                readIntText(p, buf);
                skipWhitespaceIfAny(buf);
                assertChar(',', buf);
                skipWhitespaceIfAny(buf);
                readIntText(sc, buf);
                return {p, sc};
            };
            auto [old_precision, old_scale] = parse(old_str);
            auto [new_precision, new_scale] = parse(new_str);
            if (old_precision <= new_precision && old_scale == new_scale)
                return true;
        }
    }

    if (!old_type.isString() && !new_type.isString())
    {
        auto old_complex_type = old_type.extract<Poco::JSON::Object::Ptr>();
        auto new_complex_type = new_type.extract<Poco::JSON::Object::Ptr>();

        if (old_complex_type && new_complex_type && old_complex_type->has("precision") && new_complex_type->has("precision") &&
            (old_complex_type->getValue<Int32>("precision") <= new_complex_type->getValue<Int32>("precision") &&
             old_complex_type->getValue<Int32>("scale") == new_complex_type->getValue<Int32>("scale")))
        {
            return true;
        }
    }

    return false;
}


/// Recursively drop the field ids Iceberg assigns to nested elements of a complex type.
/// `getIcebergType` allocates them from a running counter, so regenerating the same
/// ClickHouse type with a different counter start yields a different - but structurally
/// identical - descriptor. Removing the ids makes such descriptors comparable.
void stripNestedFieldIds(Poco::JSON::Object::Ptr type_object)
{
    for (const auto & id_field : {Iceberg::f_id, Iceberg::f_element_id, Iceberg::f_key_id, Iceberg::f_value_id})
        type_object->remove(id_field);

    for (const auto & nested_field : {Iceberg::f_element, Iceberg::f_key, Iceberg::f_value, Iceberg::f_type})
    {
        if (!type_object->has(nested_field))
            continue;
        auto nested = type_object->get(nested_field);
        if (nested.isString())
            continue;
        if (auto nested_object = nested.extract<Poco::JSON::Object::Ptr>())
            stripNestedFieldIds(nested_object);
    }

    if (type_object->has(Iceberg::f_fields))
    {
        auto fields = type_object->getArray(Iceberg::f_fields);
        for (UInt32 i = 0; i < fields->size(); ++i)
        {
            if (auto field = fields->getObject(i))
                stripNestedFieldIds(field);
        }
    }
}

/// Like `icebergTypesEqual`, but ignores the field ids embedded in complex types.
/// Used to recognize a type that a previous attempt already wrote, where the ids
/// were allocated from a lower `last-column-id` than the one we would use now.
bool icebergTypesEqualIgnoringIds(Poco::Dynamic::Var old_type, Poco::Dynamic::Var new_type)
{
    if (old_type.isString() && new_type.isString())
        return old_type.extract<String>() == new_type.extract<String>();

    if (old_type.isString() || new_type.isString())
        return false;

    auto old_object = old_type.extract<Poco::JSON::Object::Ptr>();
    auto new_object = new_type.extract<Poco::JSON::Object::Ptr>();
    if (!old_object || !new_object)
        return false;

    auto old_stripped = deepCopy(old_object);
    auto new_stripped = deepCopy(new_object);
    stripNestedFieldIds(old_stripped);
    stripNestedFieldIds(new_stripped);

    std::ostringstream oss_old; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    std::ostringstream oss_new; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    old_stripped->stringify(oss_old);
    new_stripped->stringify(oss_new);
    return oss_old.str() == oss_new.str();
}

/// The Iceberg spec marks `snapshots`, `metadata-log` and `snapshot-log` as optional, so table
/// metadata written by another engine may omit any of them - typically for a table that has never
/// been written to. Return the array, creating an empty one when the field is absent, so that
/// appending to it does not depend on whoever created the table.
Poco::JSON::Array::Ptr getOrCreateArray(Poco::JSON::Object::Ptr object, const char * field_name)
{
    auto array = object->getArray(field_name);
    if (array.isNull())
    {
        array = new Poco::JSON::Array;
        object->set(field_name, array);
    }
    return array;
}

/// Allocate the next schema id as max(existing schema ids) + 1 to avoid
/// collisions when current-schema-id is not the highest in the list.
Int32 getNextSchemaId(Poco::JSON::Object::Ptr metadata_object)
{
    Int32 max_id = 0;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
        max_id = std::max(max_id, schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id));
    return max_id + 1;
}

}

MetadataGenerator::MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_)
    : metadata_object(metadata_object_)
    , gen(randomSeed())
    , dis(1, std::numeric_limits<Int64>::max())
{
}

Int64 MetadataGenerator::getMaxSequenceNumber()
{
    /// Use the authoritative top-level field per Iceberg V2 spec.
    /// Iterating snapshots is unreliable when catalogs prune snapshot history.
    if (metadata_object->has(Iceberg::f_last_sequence_number))
        return metadata_object->getValue<Int64>(Iceberg::f_last_sequence_number);

    /// `snapshots` is optional: a table with no snapshot history has no sequence number to report.
    auto snapshots = metadata_object->getArray(Iceberg::f_snapshots);
    if (snapshots.isNull())
        return 0;

    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::findCurrentSchema() const
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
            return schemas->getObject(i);
    }
    return nullptr;
}

Poco::JSON::Object::Ptr MetadataGenerator::getCurrentSchema() const
{
    auto current_schema = findCurrentSchema();
    if (!current_schema)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Not found schema with id {}",
            metadata_object->getValue<Int32>(Iceberg::f_current_schema_id));
    return current_schema;
}

bool MetadataGenerator::isAddColumnApplied(const String & column_name, DataTypePtr type) const
{
    auto current_schema = findCurrentSchema();
    if (!current_schema)
        return false;

    Int32 unused_field_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    auto expected_type = Iceberg::getIcebergType(type, unused_field_id);

    auto fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(i);
        if (field->getValue<String>(Iceberg::f_name) != column_name)
            continue;
        /// The stored descriptor was produced from a lower `last-column-id` than the one we
        /// just used, so the ids of nested elements differ even for the very same type.
        return field->getValue<bool>(Iceberg::f_required) == expected_type.second
            && icebergTypesEqualIgnoringIds(field->get(Iceberg::f_type), expected_type.first);
    }
    return false;
}

bool MetadataGenerator::isDropColumnApplied(const String & column_name) const
{
    auto current_schema = findCurrentSchema();
    if (!current_schema)
        return false;

    auto fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        if (fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
            return false;
    }
    return true;
}

bool MetadataGenerator::isRenameColumnApplied(const String & column_name, const String & new_column_name) const
{
    auto current_schema = findCurrentSchema();
    if (!current_schema)
        return false;

    bool found_new_name = false;
    auto fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        auto name = fields->getObject(i)->getValue<String>(Iceberg::f_name);
        if (name == column_name)
            return false;
        if (name == new_column_name)
            found_new_name = true;
    }
    return found_new_name;
}

bool MetadataGenerator::isModifyColumnApplied(const String & column_name, DataTypePtr type) const
{
    auto current_schema = findCurrentSchema();
    if (!current_schema)
        return false;

    Int32 unused_field_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    auto expected_type = Iceberg::getIcebergType(type, unused_field_id);

    auto fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(i);
        if (field->getValue<String>(Iceberg::f_name) != column_name)
            continue;
        return field->getValue<bool>(Iceberg::f_required) == expected_type.second
            && icebergTypesEqualIgnoringIds(field->get(Iceberg::f_type), expected_type.first);
    }
    return false;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    /// `snapshots` is optional: with no snapshot history there is no parent to find.
    auto snapshots = metadata_object->getArray(Iceberg::f_snapshots);
    if (snapshots.isNull())
        return nullptr;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        if (snapshot_id == parent_snapshot_id)
            return snapshot;
    }
    return nullptr;
}

MetadataGenerator::NextMetadataResult MetadataGenerator::generateNextMetadata(
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathFromMetadata & metadata_file_path,
    Int64 parent_snapshot_id,
    Int64 added_files,
    Int64 added_records,
    Int64 added_files_size,
    Int64 num_partitions,
    Int64 added_delete_files,
    Int64 num_deleted_rows,
    std::optional<Int64> user_defined_snapshot_id,
    std::optional<Int64> user_defined_timestamp,
    bool is_truncate)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, sequence_number);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int64 snapshot_id = user_defined_snapshot_id.value_or(static_cast<Int64>(dis(gen)));

    auto manifest_list_path = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    Int64 timestamp = user_defined_timestamp.value_or(ms.count());
    new_snapshot->set(Iceberg::f_timestamp_ms, timestamp);
    metadata_object->set(Iceberg::f_last_updated_ms, timestamp);

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    if (is_truncate)
    {
        summary->set(Iceberg::f_operation, Iceberg::f_overwrite);
        Int32 prev_total_records = parent_snapshot && parent_snapshot->has(Iceberg::f_summary) && parent_snapshot->getObject(Iceberg::f_summary)->has(Iceberg::f_total_records) ? std::stoi(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(Iceberg::f_total_records)) : 0;
        Int32 prev_total_data_files = parent_snapshot && parent_snapshot->has(Iceberg::f_summary) && parent_snapshot->getObject(Iceberg::f_summary)->has(Iceberg::f_total_data_files) ? std::stoi(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(Iceberg::f_total_data_files)) : 0;

        summary->set(Iceberg::f_deleted_records, std::to_string(prev_total_records));
        summary->set(Iceberg::f_deleted_data_files, std::to_string(prev_total_data_files));
    }
    else if (num_deleted_rows == 0)
    {
        summary->set(Iceberg::f_operation, Iceberg::f_append);
        summary->set(Iceberg::f_added_data_files, std::to_string(added_files));
        summary->set(Iceberg::f_added_records, std::to_string(added_records));
        summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
        summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));
    }
    else
    {
        summary->set(Iceberg::f_operation, Iceberg::f_overwrite);
        summary->set(Iceberg::f_added_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_position_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
        summary->set(Iceberg::f_added_position_deletes, std::to_string(num_deleted_rows));
        summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));
    }

    auto sum_with_parent_snapshot = [&](const char * field_name, Int64 snapshot_value)
    {
        if (is_truncate)
        {
            summary->set(field_name, std::to_string(0));
            return;
        }
        Int64 prev_value = parent_snapshot && parent_snapshot->has(Iceberg::f_summary) && parent_snapshot->getObject(Iceberg::f_summary)->has(field_name) ? parse<Int64>(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(field_name)) : 0;
        summary->set(field_name, std::to_string(prev_value + snapshot_value));
    };

    sum_with_parent_snapshot(Iceberg::f_total_records, added_records);
    sum_with_parent_snapshot(Iceberg::f_total_files_size, added_files_size);
    sum_with_parent_snapshot(Iceberg::f_total_data_files, added_files);
    sum_with_parent_snapshot(Iceberg::f_total_delete_files, added_delete_files);
    sum_with_parent_snapshot(Iceberg::f_total_position_deletes, num_deleted_rows);
    sum_with_parent_snapshot(Iceberg::f_total_equality_deletes, 0);
    new_snapshot->set(Iceberg::f_summary, summary);

    new_snapshot->set(Iceberg::f_schema_id, metadata_object->getValue<Int32>(Iceberg::f_current_schema_id));
    new_snapshot->set(Iceberg::f_manifest_list, manifest_list_path.serialize());

    if (format_version >= 3)
    {
        Int64 next_row_id = metadata_object->has(Iceberg::f_next_row_id) && !metadata_object->isNull(Iceberg::f_next_row_id)
            ? metadata_object->getValue<Int64>(Iceberg::f_next_row_id)
            : 0;
        new_snapshot->set(Iceberg::f_first_row_id, next_row_id);
        new_snapshot->set(Iceberg::f_added_rows, added_records);
        metadata_object->set(Iceberg::f_next_row_id, next_row_id + added_records);
    }

    getOrCreateArray(metadata_object, Iceberg::f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);

    if (!metadata_object->has(Iceberg::f_refs))
        metadata_object->set(Iceberg::f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));

    if (!metadata_object->getObject(Iceberg::f_refs)->has(Iceberg::f_main))
    {
        Poco::JSON::Object::Ptr branch = new Poco::JSON::Object;
        branch->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        branch->set(Iceberg::f_type, Iceberg::f_branch);

        metadata_object->getObject(Iceberg::f_refs)->set(Iceberg::f_main, branch);
    }
    else
        metadata_object->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->set(Iceberg::f_metadata_snapshot_id, snapshot_id);

    {
        Poco::JSON::Object::Ptr new_metadata_item = new Poco::JSON::Object;
        new_metadata_item->set(Iceberg::f_metadata_file, metadata_file_path.serialize());
        new_metadata_item->set(Iceberg::f_timestamp_ms, timestamp);
        getOrCreateArray(metadata_object, Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, timestamp);
        getOrCreateArray(metadata_object, Iceberg::f_snapshot_log)->add(new_snapshot_item);
    }

    if (added_delete_files > 0)
    {
        if (!metadata_object->has(Iceberg::f_properties))
        {
            Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
            metadata_object->set(Iceberg::f_properties, properties);
        }
        auto properties = metadata_object->getObject(Iceberg::f_properties);
        properties->set("owner", "root");
        properties->set("write.delete.mode", "merge-on-read");
        properties->set("write.merge.mode", "merge-on-read");
        properties->set("write.update.mode", "merge-on-read");
    }
    return {new_snapshot, manifest_list_path};
}

void MetadataGenerator::generateDropColumnMetadata(const String & column_name)
{
    const auto next_schema_id = getNextSchemaId(metadata_object);

    auto current_schema = deepCopy(getCurrentSchema());

    auto fields = current_schema->getArray(Iceberg::f_fields);
    UInt32 index_to_drop = static_cast<UInt32>(fields->size());
    Int32 dropped_field_id = -1;
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        if (fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
        {
            index_to_drop = i;
            dropped_field_id = fields->getObject(i)->getValue<Int32>(Iceberg::f_id);
            break;
        }
    }
    if (index_to_drop == fields->size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);

    /// Reject the drop if the column is referenced by the active sort order.
    if (metadata_object->has(Iceberg::f_sort_orders) && metadata_object->has(Iceberg::f_default_sort_order_id))
    {
        auto default_sort_order_id = metadata_object->getValue<Int64>(Iceberg::f_default_sort_order_id);
        if (default_sort_order_id != 0)
        {
            auto sort_orders = metadata_object->getArray(Iceberg::f_sort_orders);
            for (UInt32 i = 0; i < sort_orders->size(); ++i)
            {
                auto sort_order = sort_orders->getObject(i);
                if (sort_order->getValue<Int64>(Iceberg::f_order_id) != default_sort_order_id)
                    continue;
                if (!sort_order->has(Iceberg::f_fields))
                    break;
                auto sort_fields = sort_order->getArray(Iceberg::f_fields);
                for (UInt32 j = 0; j < sort_fields->size(); ++j)
                {
                    auto sf = sort_fields->getObject(j);
                    if (sf->has(Iceberg::f_source_id) && sf->getValue<Int32>(Iceberg::f_source_id) == dropped_field_id)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Cannot drop column '{}' (field id {}): it is referenced by the active sort order",
                            column_name, dropped_field_id);
                }
                break;
            }
        }
    }

    /// Reject the drop if the column is referenced by the active partition spec.
    if (metadata_object->has(Iceberg::f_partition_specs) && metadata_object->has(Iceberg::f_default_spec_id))
    {
        auto default_spec_id = metadata_object->getValue<Int64>(Iceberg::f_default_spec_id);
        auto partition_specs = metadata_object->getArray(Iceberg::f_partition_specs);
        for (UInt32 i = 0; i < partition_specs->size(); ++i)
        {
            auto spec = partition_specs->getObject(i);
            if (spec->getValue<Int64>(Iceberg::f_spec_id) != default_spec_id)
                continue;
            if (!spec->has(Iceberg::f_fields))
                break;
            auto spec_fields = spec->getArray(Iceberg::f_fields);
            for (UInt32 j = 0; j < spec_fields->size(); ++j)
            {
                auto pf = spec_fields->getObject(j);
                if (pf->has(Iceberg::f_source_id) && pf->getValue<Int32>(Iceberg::f_source_id) == dropped_field_id)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot drop column '{}' (field id {}): it is referenced by the active partition spec",
                        column_name, dropped_field_id);
            }
            break;
        }
    }

    current_schema->getArray(Iceberg::f_fields)->remove(index_to_drop);
    current_schema->set(Iceberg::f_schema_id, next_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, next_schema_id);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateAddColumnMetadata(const String & column_name, DataTypePtr type)
{
    if (!type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow to add non-nullable columns");
    const auto next_schema_id = getNextSchemaId(metadata_object);

    auto current_schema = deepCopy(getCurrentSchema());

    auto existing_fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < existing_fields->size(); ++i)
    {
        if (existing_fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} already exists", column_name);
    }

    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);

    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    Poco::JSON::Object::Ptr new_field = new Poco::JSON::Object;
    new_field->set(Iceberg::f_id, last_column_id + 1);
    new_field->set(Iceberg::f_name, column_name);
    new_field->set(Iceberg::f_required, new_type.second);
    new_field->set(Iceberg::f_type, new_type.first);

    metadata_object->set(Iceberg::f_last_column_id, last_column_id + 1);

    current_schema->getArray(Iceberg::f_fields)->add(new_field);
    current_schema->set(Iceberg::f_schema_id, next_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, next_schema_id);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

bool MetadataGenerator::generateModifyColumnMetadata(const String & column_name, DataTypePtr type, ContextPtr context)
{
    auto current_schema = getCurrentSchema();

    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            if (current_field->getValue<bool>(Iceberg::f_required) == new_type.second
                && icebergTypesEqualIgnoringIds(current_field->get(Iceberg::f_type), new_type.first))
            {
                auto existing_iceberg_type = current_field->get(Iceberg::f_type);
                if (existing_iceberg_type.isString())
                {
                    auto reconstructed_ch_type = Iceberg::IcebergSchemaProcessor::getSimpleType(
                        existing_iceberg_type.extract<String>(),
                        context,
                        context->getSettingsRef()[Setting::allow_experimental_geo_types_in_iceberg]);
                    if (!current_field->getValue<bool>(Iceberg::f_required) && reconstructed_ch_type->canBeInsideNullable())
                        reconstructed_ch_type = makeNullable(reconstructed_ch_type);

                    if (reconstructed_ch_type->equals(*type))
                        return false;

                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot MODIFY COLUMN '{}' from {} to {}: both map to the same Iceberg type '{}' "
                        "so the change cannot be recorded in the Iceberg schema",
                        column_name,
                        reconstructed_ch_type->getName(),
                        type->getName(),
                        existing_iceberg_type.extract<String>());
                }

                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot MODIFY COLUMN '{}': the requested and existing types both map to the same "
                    "Iceberg complex type, and the change cannot be recorded in the Iceberg schema",
                    column_name);
            }

            if (!checkValidSchemaEvolution(current_field->get(Iceberg::f_type), new_type.first))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow schema evolution to type {}", type->getPrettyName());

            if (!current_field->getValue<bool>(Iceberg::f_required) && !type->isNullable())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow change type from nullable to non-nullable {}", type->getPrettyName());

            const auto next_schema_id = getNextSchemaId(metadata_object);

            current_schema = deepCopy(current_schema);
            schema_fields = current_schema->getArray(Iceberg::f_fields);
            current_field = schema_fields->getObject(i);

            current_field->set(Iceberg::f_type, new_type.first);
            current_field->set(Iceberg::f_required, new_type.second);

            metadata_object->set(Iceberg::f_current_schema_id, next_schema_id);
            current_schema->set(Iceberg::f_schema_id, next_schema_id);
            metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
            return true;
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} not found in schema", column_name);
}

void MetadataGenerator::generateRenameColumnMetadata(const String & column_name, const String & new_column_name)
{
    auto current_schema = deepCopy(getCurrentSchema());

    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        if (schema_fields->getObject(i)->getValue<String>(Iceberg::f_name) == new_column_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} already exists", new_column_name);
    }

    bool found = false;
    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            current_field->set(Iceberg::f_name, new_column_name);
            found = true;
            break;
        }
    }

    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);

    const auto next_schema_id = getNextSchemaId(metadata_object);
    metadata_object->set(Iceberg::f_current_schema_id, next_schema_id);
    current_schema->set(Iceberg::f_schema_id, next_schema_id);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

}

#endif
