#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#include <climits>
#include <optional>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

#include <Common/randomSeed.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
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

/// Read a numeric `total-*` field from the parent snapshot's summary, returning std::nullopt when absent or null.
std::optional<Int64> readParentTotal(Poco::JSON::Object::Ptr parent_snapshot, const char * field_name)
{
    if (!parent_snapshot || !parent_snapshot->has(Iceberg::f_summary))
        return std::nullopt;
    auto parent_summary = parent_snapshot->getObject(Iceberg::f_summary);
    if (!parent_summary || !parent_summary->has(field_name) || parent_summary->isNull(field_name))
        return std::nullopt;
    return parse<Int64>(parent_summary->getValue<String>(field_name));
}

/// Write the standard `total-*` counters into `summary` by adding each per-field delta to the corresponding parent value.
void setSnapshotTotals(
    Poco::JSON::Object::Ptr summary,
    Poco::JSON::Object::Ptr parent_snapshot,
    Int64 added_records,
    Int64 added_files_size,
    Int64 added_data_files,
    Int64 added_delete_files,
    Int64 added_position_deletes,
    Int64 added_equality_deletes)
{
    /// Data totals (records, files size, data files) describe the whole table state.
    auto set_data_total = [&](const char * field_name, Int64 added)
    {
        /// No parent snapshot: this is the base snapshot, so its total is exactly what it adds.
        if (!parent_snapshot)
        {
            summary->set(field_name, std::to_string(added));
            return;
        }
        /// The parent omits this data total, so the new table-wide total cannot be derived: fail the rewrite instead of corrupting the summary.
        auto parent_value = readParentTotal(parent_snapshot, field_name);
        if (!parent_value.has_value())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot derive Iceberg snapshot total '{}': the parent snapshot's summary omits it",
                field_name);
        summary->set(field_name, std::to_string(*parent_value + added));
    };
    /// Delete-family totals: a missing parent counter means "none", so treating it as 0 is safe.
    auto set_delete_total = [&](const char * field_name, Int64 added)
    {
        summary->set(field_name, std::to_string(readParentTotal(parent_snapshot, field_name).value_or(0) + added));
    };
    set_data_total(Iceberg::f_total_records, added_records);
    set_data_total(Iceberg::f_total_files_size, added_files_size);
    set_data_total(Iceberg::f_total_data_files, added_data_files);
    set_delete_total(Iceberg::f_total_delete_files, added_delete_files);
    set_delete_total(Iceberg::f_total_position_deletes, added_position_deletes);
    set_delete_total(Iceberg::f_total_equality_deletes, added_equality_deletes);
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

    {
        auto old_complex_type = old_type.extract<Poco::JSON::Object::Ptr>();
        auto new_complex_type = new_type.extract<Poco::JSON::Object::Ptr>();

        if (old_complex_type && new_complex_type && old_complex_type->has("precision") && new_complex_type->has("precision") &&
            (old_complex_type->getValue<Int32>("precision") <= new_complex_type->getValue<Int32>("precision") &&
             old_complex_type->getValue<Int32>("scale") <= new_complex_type->getValue<Int32>("scale")))
        {
            return true;
        }
    }

    return false;
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
    /// Use the authoritative top-level field per Iceberg V2 spec, since iterating snapshots is unreliable when history is pruned.
    if (metadata_object->has(Iceberg::f_last_sequence_number))
        return metadata_object->getValue<Int64>(Iceberg::f_last_sequence_number);

    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
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
    std::optional<Int64> user_defined_timestamp)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);

    /// These arrays are optional per the Iceberg spec, so external metadata may omit them.
    /// Seed an empty one (as Array::Ptr so getArray/extract see the right type tag) before use.
    for (const auto * field : {Iceberg::f_metadata_log, Iceberg::f_snapshot_log})
        if (!metadata_object->has(field))
            metadata_object->set(field, Poco::JSON::Array::Ptr(new Poco::JSON::Array));

    /// `snapshots` may only be seeded when the table has no current snapshot: with a live
    /// parent snapshot the commit relies on finding its manifest list in `snapshots`, and an
    /// empty list would silently unlink all previously committed data instead of failing.
    if (!metadata_object->has(Iceberg::f_snapshots))
    {
        if (parent_snapshot_id >= 0)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Metadata has a current snapshot with id {} but no `snapshots` list",
                parent_snapshot_id);
        metadata_object->set(Iceberg::f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    }

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
    summary->set(Iceberg::f_operation, num_deleted_rows == 0 ? Iceberg::f_append : Iceberg::f_overwrite);
    summary->set(Iceberg::f_added_data_files, std::to_string(added_files));
    summary->set(Iceberg::f_added_records, std::to_string(added_records));
    summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
    summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));
    if (num_deleted_rows != 0)
    {
        summary->set(Iceberg::f_added_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_position_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_position_deletes, std::to_string(num_deleted_rows));
    }

    setSnapshotTotals(
        summary,
        parent_snapshot,
        /*added_records=*/added_records,
        /*added_files_size=*/added_files_size,
        /*added_data_files=*/added_files,
        /*added_delete_files=*/added_delete_files,
        /*added_position_deletes=*/num_deleted_rows,
        /*added_equality_deletes=*/0);
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

    metadata_object->getArray(Iceberg::f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);

    if (!metadata_object->has(Iceberg::f_refs))
        metadata_object->set(Iceberg::f_refs, new Poco::JSON::Object);

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
        metadata_object->getArray(Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_snapshot_log)->add(new_snapshot_item);
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

MetadataGenerator::NextMetadataResult MetadataGenerator::generateManifestOnlySnapshot(
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathFromMetadata & metadata_file_path,
    Int64 parent_snapshot_id)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);

    /// These arrays are optional per the Iceberg spec, so external metadata may omit them.
    /// Seed an empty one (as Array::Ptr so getArray/extract see the right type tag) before use,
    /// mirroring `generateNextMetadata`; otherwise appending to a missing log dereferences a null array.
    for (const auto * field : {Iceberg::f_metadata_log, Iceberg::f_snapshot_log})
        if (!metadata_object->has(field))
            metadata_object->set(field, Poco::JSON::Array::Ptr(new Poco::JSON::Array));

    /// A manifest-only rewrite always runs against a table with a current snapshot, so `snapshots`
    /// must already be present. Guard the same way as `generateNextMetadata` instead of dereferencing
    /// a null array below: with a live parent snapshot a missing `snapshots` list is corrupt metadata.
    if (!metadata_object->has(Iceberg::f_snapshots))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Metadata has a current snapshot with id {} but no `snapshots` list",
            parent_snapshot_id);

    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, sequence_number);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int64 snapshot_id = static_cast<Int64>(dis(gen));

    auto manifest_list_path = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    Int64 timestamp = ms.count();
    new_snapshot->set(Iceberg::f_timestamp_ms, timestamp);
    metadata_object->set(Iceberg::f_last_updated_ms, timestamp);

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);

    /// Manifest-only rewrite: all added-* deltas are zero so `total-*` counters are inherited unchanged from the parent.
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    summary->set(Iceberg::f_operation, Iceberg::f_replace);
    summary->set(Iceberg::f_added_data_files, "0");
    summary->set(Iceberg::f_added_records, "0");
    summary->set(Iceberg::f_added_files_size, "0");
    summary->set(Iceberg::f_changed_partition_count, "0");

    setSnapshotTotals(
        summary,
        parent_snapshot,
        /*added_records=*/0,
        /*added_files_size=*/0,
        /*added_data_files=*/0,
        /*added_delete_files=*/0,
        /*added_position_deletes=*/0,
        /*added_equality_deletes=*/0);
    new_snapshot->set(Iceberg::f_summary, summary);

    new_snapshot->set(Iceberg::f_schema_id, metadata_object->getValue<Int32>(Iceberg::f_current_schema_id));
    new_snapshot->set(Iceberg::f_manifest_list, manifest_list_path.serialize());

    metadata_object->getArray(Iceberg::f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);

    if (!metadata_object->has(Iceberg::f_refs))
        metadata_object->set(Iceberg::f_refs, new Poco::JSON::Object);

    if (!metadata_object->getObject(Iceberg::f_refs)->has(Iceberg::f_main))
    {
        Poco::JSON::Object::Ptr branch = new Poco::JSON::Object;
        branch->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        branch->set(Iceberg::f_type, Iceberg::f_branch);
        metadata_object->getObject(Iceberg::f_refs)->set(Iceberg::f_main, branch);
    }
    else
    {
        metadata_object->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    }

    {
        Poco::JSON::Object::Ptr new_metadata_item = new Poco::JSON::Object;
        new_metadata_item->set(Iceberg::f_metadata_file, metadata_file_path.serialize());
        new_metadata_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_snapshot_log)->add(new_snapshot_item);
    }

    return {new_snapshot, manifest_list_path};
}

void MetadataGenerator::generateDropColumnMetadata(const String & column_name)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

    auto fields = current_schema->getArray(Iceberg::f_fields);
    UInt32 index_to_drop = static_cast<UInt32>(fields->size());
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        if (fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
        {
            index_to_drop = i;
            break;
        }
    }
    if (index_to_drop == fields->size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);
    current_schema->getArray(Iceberg::f_fields)->remove(index_to_drop);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateAddColumnMetadata(const String & column_name, DataTypePtr type)
{
    if (!type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow to add non-nullable columns");
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

    auto existing_fields = current_schema->getArray(Iceberg::f_fields);
    for (UInt32 i = 0; i < existing_fields->size(); ++i)
    {
        if (existing_fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} already exists", column_name);
    }

    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    metadata_object->set(Iceberg::f_last_column_id, last_column_id + 1);

    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    Poco::JSON::Object::Ptr new_field = new Poco::JSON::Object;
    new_field->set(Iceberg::f_id, last_column_id + 1);
    new_field->set(Iceberg::f_name, column_name);
    new_field->set(Iceberg::f_required, new_type.second);
    new_field->set(Iceberg::f_type, new_type.first);

    current_schema->getArray(Iceberg::f_fields)->add(new_field);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateModifyColumnMetadata(const String & column_name, DataTypePtr type)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);
    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);

    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    bool found = false;
    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            if (!checkValidSchemaEvolution(current_field->get(Iceberg::f_type), new_type.first))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow schema evolution to type {}", type->getPrettyName());

            auto old_type = deepCopy(current_field);
            current_field->set(Iceberg::f_type, new_type.first);
            if (!current_field->getValue<bool>(Iceberg::f_required) && !type->isNullable())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow change type from nullable to non-nullable {}", type->getPrettyName());

            current_field->set(Iceberg::f_required, new_type.second);
            found = true;
            break;
        }
    }

    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);

    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateRenameColumnMetadata(const String & column_name, const String & new_column_name)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

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

    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

}

#endif
