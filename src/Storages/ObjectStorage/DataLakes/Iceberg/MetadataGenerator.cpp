#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

#include <Common/randomSeed.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    , dis(0, INT32_MAX)
{
}

Int64 MetadataGenerator::getMaxSequenceNumber()
{
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
    const String & metadata_filename,
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
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, getMaxSequenceNumber() + 1);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int64 snapshot_id = user_defined_snapshot_id.value_or(static_cast<Int64>(dis(gen)));

    auto [manifest_list_name, storage_manifest_list_name] = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    Int64 timestamp = user_defined_timestamp.value_or(ms.count());
    new_snapshot->set(Iceberg::f_timestamp_ms, timestamp);
    metadata_object->set(Iceberg::f_last_updated_ms, timestamp);

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    if (num_deleted_rows == 0)
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
        Int64 prev_value = parent_snapshot ? parse<Int64>(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(field_name)) : 0;
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
    new_snapshot->set(Iceberg::f_manifest_list, manifest_list_name);

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
        new_metadata_item->set(Iceberg::f_metadata_file, metadata_filename);
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
    return {new_snapshot, manifest_list_name, storage_manifest_list_name};
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
            break;
        }
    }
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
