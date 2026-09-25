#include <Server/IcebergRESTCatalog/IcebergRESTCatalogTableMetadata.h>

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Poco/JSON/Array.h>

#include <chrono>
#include <set>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace Iceberg;

namespace
{

/// Partition field ids start here by Iceberg convention. `last-partition-id` of a table without partition fields is one less.
constexpr Int64 PARTITION_FIELD_ID_START = 1000;

Int64 getInteger(const Poco::JSON::Object & object, const String & key, const String & what)
{
    if (!object.has(key) || !object.get(key).isInteger())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have an integer '{}'", what, key);
    return object.getValue<Int64>(key);
}

Poco::JSON::Array::Ptr getArray(const Poco::JSON::Object & object, const String & key, const String & what)
{
    auto array = object.getArray(key);
    if (!array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have an array '{}'", what, key);
    return array;
}

void collectFieldId(const Poco::JSON::Object & holder, const String & key, std::set<Int64> & ids)
{
    const auto id = getInteger(holder, key, "Every schema field");
    if (!ids.insert(id).second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate field id {} in schema", id);
}

void collectNestedFieldIds(const Poco::Dynamic::Var & type, std::set<Int64> & ids)
{
    if (type.type() != typeid(Poco::JSON::Object::Ptr))
        return;

    const auto object = type.extract<Poco::JSON::Object::Ptr>();
    const auto kind = object->optValue<String>(f_type, "");
    if (kind == f_struct)
    {
        for (const auto & field : *getArray(*object, f_fields, "A struct type"))
        {
            if (field.type() != typeid(Poco::JSON::Object::Ptr))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must be an object");
            const auto field_object = field.extract<Poco::JSON::Object::Ptr>();
            collectFieldId(*field_object, f_id, ids);
            if (!field_object->has(f_type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must have a 'type'");
            collectNestedFieldIds(field_object->get(f_type), ids);
        }
    }
    else if (kind == f_list)
    {
        collectFieldId(*object, f_element_id, ids);
        collectNestedFieldIds(object->get(f_element), ids);
    }
    else if (kind == f_map)
    {
        collectFieldId(*object, f_key_id, ids);
        collectNestedFieldIds(object->get(f_key), ids);
        collectFieldId(*object, f_value_id, ids);
        collectNestedFieldIds(object->get(f_value), ids);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown nested type '{}' in schema", kind);
}

std::vector<Poco::JSON::Object::Ptr> validateSpecFields(const Poco::JSON::Object & spec, const String & spec_name, const std::set<Int64> & schema_ids)
{
    const auto what = fmt::format("'{}'", spec_name);
    std::vector<Poco::JSON::Object::Ptr> fields;
    for (const auto & field : *getArray(spec, f_fields, what))
    {
        if (field.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every field of {} must be an object", what);
        const auto field_object = field.extract<Poco::JSON::Object::Ptr>();

        // Validate source-id.
        const auto source_id = getInteger(*field_object, f_source_id, what + " field");
        if (!schema_ids.contains(source_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} references unknown source-id {}", what, source_id);
        fields.push_back(field_object);
    }
    return fields;
}

/// Returns `last-partition-id`: the largest `field-id` of the spec, or the conventional value for an unpartitioned table.
Int64 validatePartitionSpec(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    Int64 last_partition_id = PARTITION_FIELD_ID_START - 1;
    for (const auto & field : validateSpecFields(spec, "partition-spec", schema_ids))
        last_partition_id = std::max(last_partition_id, getInteger(*field, f_field_id, "'partition-spec' field"));
    return last_partition_id;
}

void validateWriteOrder(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    validateSpecFields(spec, "write-order", schema_ids);
}

std::set<Int64> getFieldIds(const Poco::JSON::Object::Ptr & schema)
{
    std::set<Int64> ids;
    collectNestedFieldIds(Poco::Dynamic::Var(schema), ids);
    return ids;
}

/// Checks the schema is a struct and marks it as schema 0.
void prepareSchema(Poco::JSON::Object::Ptr schema)
{
    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be an object");
    if (schema->optValue<String>(f_type, f_struct) != f_struct)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be a struct type");
    schema->set(f_type, f_struct);
    schema->set(f_schema_id, 0);
}

/// Replaces a missing spec with an empty one and marks it as spec 0 under `id_key`.
Poco::JSON::Object::Ptr prepareSpec(Poco::JSON::Object::Ptr spec, const String & id_key)
{
    if (!spec)
    {
        spec = new Poco::JSON::Object;
        spec->set(f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    }
    spec->set(id_key, 0);
    return spec;
}

}

Poco::JSON::Object::Ptr buildInitialTableMetadata(
    const String & uuid,
    const String & location,
    Poco::JSON::Object::Ptr schema,
    Poco::JSON::Object::Ptr partition_spec,
    Poco::JSON::Object::Ptr write_order,
    std::map<String, String> properties)
{
    /// TODO: support format version 3. The initial file needs `next-row-id: 0`, and the commit path needs v3 handling.
    if (auto it = properties.find(f_format_version); it != properties.end())
    {
        if (it->second != "2")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only format-version 2 is supported, got '{}'", it->second);
        properties.erase(it);
    }

    prepareSchema(schema);
    const auto field_ids = getFieldIds(schema);
    const Int64 last_column_id = field_ids.empty() ? 0 : *field_ids.rbegin();

    partition_spec = prepareSpec(partition_spec, f_spec_id);
    const auto last_partition_id = validatePartitionSpec(*partition_spec, field_ids);

    write_order = prepareSpec(write_order, f_order_id);
    validateWriteOrder(*write_order, field_ids);

    Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object;
    for (const auto & [key, value] : properties)
        properties_json->set(key, value);

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    /// Key order follows the Iceberg spec listing so the file reads like the ones Java writes.
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    metadata->set(f_format_version, 2);
    metadata->set(f_table_uuid, uuid);
    metadata->set(f_location, location);
    metadata->set(f_last_sequence_number, 0);
    metadata->set(f_last_updated_ms, static_cast<Int64>(now_ms));
    metadata->set(f_last_column_id, last_column_id);
    metadata->set(f_current_schema_id, 0);
    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    schemas->add(schema);
    metadata->set(f_schemas, schemas);
    metadata->set(f_default_spec_id, 0);
    Poco::JSON::Array::Ptr partition_specs = new Poco::JSON::Array;
    partition_specs->add(partition_spec);
    metadata->set(f_partition_specs, partition_specs);
    metadata->set(f_last_partition_id, last_partition_id);
    metadata->set(f_default_sort_order_id, 0);
    Poco::JSON::Array::Ptr sort_orders = new Poco::JSON::Array;
    sort_orders->add(write_order);
    metadata->set(f_sort_orders, sort_orders);
    metadata->set(f_properties, properties_json);
    metadata->set(f_current_snapshot_id, -1);
    /// Java and pyiceberg write an empty `refs` for a table without snapshots.
    metadata->set(f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));
    metadata->set(f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_partition_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    return metadata;
}

}
