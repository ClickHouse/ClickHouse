#include <Server/IcebergRESTCatalog/IcebergRESTCatalogTableMetadata.h>

#include "config.h"

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Poco/JSON/Array.h>

#include <chrono>
#include <set>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

using namespace Iceberg;

namespace
{

/// Partition field ids start here by Iceberg convention. `last-partition-id` of a table without partition fields is one less.
constexpr Int64 PARTITION_FIELD_ID_START = 1000;

Int64 getInteger(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key) || !object.get(key).isInteger())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-integer '{}'", key);
    return object.getValue<Int64>(key);
}

Poco::JSON::Array::Ptr getArray(const Poco::JSON::Object & object, const String & key)
{
    auto array = object.getArray(key);
    if (!array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-array '{}'", key);
    return array;
}

void collectFieldId(const Poco::JSON::Object & holder, const String & key, std::set<Int64> & ids)
{
    const auto id = getInteger(holder, key);
    if (!ids.insert(id).second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate field id {} in schema", id);
}

void checkRequiredFlag(const Poco::JSON::Object & holder, const String & key)
{
    if (!holder.has(key) || !holder.get(key).isBoolean())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-boolean '{}'", key);
}

void collectAndValidateNestedFields(const Poco::Dynamic::Var & type, std::set<Int64> & ids)
{
    if (type.isString())
    {
        /// Reuse the reader's parser so that every persisted primitive type can be read back.
        IcebergSchemaProcessor::getSimpleType(type.extract<String>());
        return;
    }

    if (type.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A schema type must be a string or an object, got '{}'", type.toString());

    const auto & object = type.extract<Poco::JSON::Object::Ptr>();
    const auto kind = object->optValue<String>(f_type, "");
    if (kind == f_struct)
    {
        for (const auto & field : *getArray(*object, f_fields))
        {
            if (field.type() != typeid(Poco::JSON::Object::Ptr))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must be an object");
            const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();
            collectFieldId(*field_object, f_id, ids);
            if (!field_object->has(f_name) || !field_object->get(f_name).isString() || field_object->getValue<String>(f_name).empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must have a non-empty string 'name'");
            checkRequiredFlag(*field_object, f_required);
            if (!field_object->has(f_type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must have a 'type'");
            collectAndValidateNestedFields(field_object->get(f_type), ids);
        }
    }
    else if (kind == f_list)
    {
        collectFieldId(*object, f_element_id, ids);
        checkRequiredFlag(*object, f_element_required);
        collectAndValidateNestedFields(object->get(f_element), ids);
    }
    else if (kind == f_map)
    {
        collectFieldId(*object, f_key_id, ids);
        collectAndValidateNestedFields(object->get(f_key), ids);
        collectFieldId(*object, f_value_id, ids);
        checkRequiredFlag(*object, f_value_required);
        collectAndValidateNestedFields(object->get(f_value), ids);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown nested type '{}' in schema", kind);
}

void checkString(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key) || !object.get(key).isString() || object.getValue<String>(key).empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty string '{}'", key);
}

/// Reuse the reader's parser so that every persisted transform can be read back.
void checkTransform(const String & transform)
{
#if USE_AVRO
    if (!parseTransformAndArgument(transform))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform '{}'", transform);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot validate transform '{}': ClickHouse was built without Avro support", transform);
#endif
}

enum class SpecKind
{
    Partition,
    SortOrder,
};

/// Checks the members of every field for the given spec kind.
std::vector<Poco::JSON::Object::Ptr> getAndValidateSpecFields(const Poco::JSON::Object & spec, SpecKind kind, const std::set<Int64> & schema_ids)
{
    const auto required_field_members = kind == SpecKind::Partition
        ? std::vector<String>{f_name, f_transform}
        : std::vector<String>{f_transform, f_direction, f_null_order};

    std::vector<Poco::JSON::Object::Ptr> fields;
    for (const auto & field : *getArray(spec, f_fields))
    {
        if (field.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every spec field must be an object");
        const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();

        /// TODO: validate the transform against the source column type, like Java's `PartitionSpec.checkCompatibility`.
        const auto source_id = getInteger(*field_object, f_source_id);
        if (!schema_ids.contains(source_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown source-id {}", source_id);
        for (const auto & key : required_field_members)
            checkString(*field_object, key);
        checkTransform(field_object->getValue<String>(f_transform));

        if (kind == SpecKind::SortOrder)
        {
            const auto direction = field_object->getValue<String>(f_direction);
            if (direction != "asc" && direction != "desc")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'direction' must be 'asc' or 'desc', got '{}'", direction);
            const auto null_order = field_object->getValue<String>(f_null_order);
            if (null_order != "nulls-first" && null_order != "nulls-last")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'null-order' must be 'nulls-first' or 'nulls-last', got '{}'", null_order);
        }

        fields.push_back(field_object);
    }
    return fields;
}

/// Validates the partition fields and assigns `field-id` 1000, 1001, etc.
/// Ignores any `field-id` sent by the client. Returns the `last-partition-id`.
Int64 getLastPartitionId(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    Int64 last_partition_id = PARTITION_FIELD_ID_START - 1;
    for (auto & field : getAndValidateSpecFields(spec, SpecKind::Partition, schema_ids))
        field->set(f_field_id, ++last_partition_id);
    return last_partition_id;
}

/// Returns the `order-id`. The spec reserves 0 for the unsorted order, so a sorted order gets 1 like in Java and pyiceberg.
Int64 getSortOrderId(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    return getAndValidateSpecFields(spec, SpecKind::SortOrder, schema_ids).empty() ? 0 : 1;
}

std::set<Int64> getFieldIds(const Poco::JSON::Object::Ptr & schema)
{
    std::set<Int64> ids;
    collectAndValidateNestedFields(Poco::Dynamic::Var(schema), ids);
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

/// Replaces a missing spec with an empty one.
Poco::JSON::Object::Ptr prepareSpec(Poco::JSON::Object::Ptr spec)
{
    if (!spec)
    {
        spec = new Poco::JSON::Object;
        spec->set(f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    }
    return spec;
}

}

/// Normalizes the request objects in place to place them in the metadata.
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

    partition_spec = prepareSpec(partition_spec);
    partition_spec->set(f_spec_id, 0);
    const auto last_partition_id = getLastPartitionId(*partition_spec, field_ids);

    write_order = prepareSpec(write_order);
    const auto sort_order_id = getSortOrderId(*write_order, field_ids);
    write_order->set(f_order_id, sort_order_id);

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
    metadata->set(f_default_sort_order_id, sort_order_id);
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
