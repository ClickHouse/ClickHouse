#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>

#include "config.h"

#include <Common/Exception.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
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
using namespace IcebergRESTCatalogJSON;

namespace
{

/// Partition field ids start here by Iceberg convention. `last-partition-id` of a table without partition fields is one less.
constexpr Int64 PARTITION_FIELD_ID_START = 1000;

void collectFieldId(const Poco::JSON::Object & holder, const String & key, std::set<Int64> & ids)
{
    const auto id = getInteger(holder, key);
    if (!ids.insert(id).second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate field id {} in schema", id);
}

void collectAndValidateNestedFields(const Poco::Dynamic::Var & type, std::set<Int64> & ids)
{
    if (type.isString())
    {
        /// Reuse type validation logic in IcebergSchemaProcessor.
        IcebergSchemaProcessor::getSimpleType(type.extract<String>());
        return;
    }

    if (type.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A schema type must be a string or an object, got '{}'", type.toString());

    const auto & object = type.extract<Poco::JSON::Object::Ptr>();
    const auto kind = object->optValue<String>(f_type, "");
    if (kind == f_struct)
    {
        /// Validate the fields of the struct.
        for (const auto & field : *getArray(*object, f_fields))
        {
            if (field.type() != typeid(Poco::JSON::Object::Ptr))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must be an object");
            const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();
            collectFieldId(*field_object, f_id, ids);

            /// Validate the struct field's name, required, and type.
            validateString(*field_object, f_name);
            validateBoolean(*field_object, f_required);

            /// Validate nested fields.
            collectAndValidateNestedFields(getValue(*field_object, f_type), ids);
        }
    }
    else if (kind == f_list)
    {
        /// Validate the list's element-id, element-required, and element.
        collectFieldId(*object, f_element_id, ids);
        validateBoolean(*object, f_element_required);
        collectAndValidateNestedFields(getValue(*object, f_element), ids);
    }
    else if (kind == f_map)
    {
        /// Validate key id and key type.
        collectFieldId(*object, f_key_id, ids);
        collectAndValidateNestedFields(getValue(*object, f_key), ids);

        /// Validate value id, nullability, and type.
        collectFieldId(*object, f_value_id, ids);
        validateBoolean(*object, f_value_required);
        collectAndValidateNestedFields(getValue(*object, f_value), ids);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown nested type '{}' in schema", kind);
}

/// Validates the schema of a new table and marks it as schema 0.
/// Returns its field ids.
std::set<Int64> prepareInitialSchema(Poco::JSON::Object::Ptr schema)
{
    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be an object");
    if (schema->optValue<String>(f_type, f_struct) != f_struct)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be a struct type");
    std::set<Int64> ids;
    collectAndValidateNestedFields(Poco::Dynamic::Var(schema), ids);
    schema->set(f_type, f_struct);
    schema->set(f_schema_id, 0);
    return ids;
}

void validateTransform(const String & transform)
{
    /// TODO: Validate the transform works with the source column type.
    /// For example, reject the `day` transform on a string column.
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
        /// Get spec field.
        if (field.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every spec field must be an object");
        const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();

        /// Validate spec references a real column.
        /// TODO: Create currently accepts partition transforms on nested fields.
        /// For example, the key of a `map<string, int>` column.
        /// However, `ChunkPartitioner` on the insert path throws when writing to such a table.
        /// We need to either reject nested source-ids or teach `ChunkPartitioner` to find nested fields.
        const auto source_id = getInteger(*field_object, f_source_id);
        if (!schema_ids.contains(source_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown source-id {}", source_id);

        /// Validate all required field members.
        for (const auto & key : required_field_members)
            validateString(*field_object, key);

        validateTransform(field_object->getValue<String>(f_transform));

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

/// Replaces a missing spec with an empty one.
void makeEmptySpecIfMissing(Poco::JSON::Object::Ptr & spec)
{
    if (!spec)
    {
        spec = new Poco::JSON::Object;
        spec->set(f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    }
}

/// Validates the first partition spec of a new table, marks it as spec 0, and assigns `field-id` 1000, 1001, etc.
/// Ignores any `field-id` sent by the client.
void prepareInitialPartitionSpec(Poco::JSON::Object::Ptr & spec, const std::set<Int64> & schema_ids)
{
    makeEmptySpecIfMissing(spec);
    spec->set(f_spec_id, 0);
    Int64 field_id = PARTITION_FIELD_ID_START;
    for (auto & field : getAndValidateSpecFields(*spec, SpecKind::Partition, schema_ids))
        field->set(f_field_id, field_id++);
}

/// Validates the first sort order of a new table and sets its `order-id`.
void prepareInitialSortOrder(Poco::JSON::Object::Ptr & spec, const std::set<Int64> & schema_ids)
{
    makeEmptySpecIfMissing(spec);
    /// Per the spec, an unsorted order gets 0. Otherwise the first sorted order gets 1.
    const Int64 order_id = getAndValidateSpecFields(*spec, SpecKind::SortOrder, schema_ids).empty() ? 0 : 1;
    spec->set(f_order_id, order_id);
}

/// Returns the largest partition `field-id`, or 999 if the spec has no fields.
Int64 getLastPartitionId(const Poco::JSON::Object & spec)
{
    Int64 last_partition_id = PARTITION_FIELD_ID_START - 1;
    for (const auto & field : *getArray(spec, f_fields))
        last_partition_id = std::max(last_partition_id, getInteger(*field.extract<Poco::JSON::Object::Ptr>(), f_field_id));
    return last_partition_id;
}

}

/// Builds the metadata of a new table from the create-table request.
Poco::JSON::Object::Ptr buildInitialTableMetadata(
    const String & uuid,
    const String & location,
    Poco::JSON::Object::Ptr schema,
    Poco::JSON::Object::Ptr partition_spec,
    Poco::JSON::Object::Ptr write_order,
    std::map<String, String> properties)
{
    /// TODO: support format version 3.
    if (auto it = properties.find(f_format_version); it != properties.end())
    {
        if (it->second != "2")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only format-version 2 is supported, got '{}'", it->second);
        properties.erase(it);
    }

    const auto field_ids = prepareInitialSchema(schema);
    prepareInitialPartitionSpec(partition_spec, field_ids);
    prepareInitialSortOrder(write_order, field_ids);

    const Int64 last_column_id = field_ids.empty() ? 0 : *field_ids.rbegin();
    const auto last_partition_id = getLastPartitionId(*partition_spec);
    const auto sort_order_id = write_order->getValue<Int64>(f_order_id);
    Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object;
    for (const auto & [key, value] : properties)
        properties_json->set(key, value);
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    /// Key order follows the Iceberg spec listing.
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    metadata->set(f_format_version, 2);
    metadata->set(f_table_uuid, uuid);
    metadata->set(f_location, location);
    metadata->set(f_last_sequence_number, 0);
    metadata->set(f_last_updated_ms, static_cast<Int64>(now_ms));
    metadata->set(f_last_column_id, last_column_id);
    metadata->set(f_current_schema_id, 0);
    metadata->set(f_schemas, makeArray(schema));
    metadata->set(f_default_spec_id, 0);
    metadata->set(f_partition_specs, makeArray(partition_spec));
    metadata->set(f_last_partition_id, last_partition_id);
    metadata->set(f_default_sort_order_id, sort_order_id);
    metadata->set(f_sort_orders, makeArray(write_order));
    metadata->set(f_properties, properties_json);
    metadata->set(f_current_snapshot_id, -1);
    metadata->set(f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));
    metadata->set(f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_partition_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    return metadata;
}

}
