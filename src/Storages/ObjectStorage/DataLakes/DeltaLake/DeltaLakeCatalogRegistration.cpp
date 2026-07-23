#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeCatalogRegistration.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Databases/DataLake/Common.h>

#include <Storages/ColumnsDescription.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <fmt/format.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Serialize a ClickHouse type to the Delta "type" JSON (string for primitives, object for array/map/struct).
Poco::Dynamic::Var deltaTypeToJSON(const DataTypePtr & full_type)
{
    DataTypePtr type = full_type->isNullable() ? removeNullable(full_type) : full_type;

    switch (type->getTypeId())
    {
        case TypeIndex::Array:
        {
            const auto & array_type = assert_cast<const DataTypeArray &>(*type);
            const auto & nested = array_type.getNestedType();
            Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
            obj->set("type", "array");
            obj->set("elementType", deltaTypeToJSON(nested));
            obj->set("containsNull", nested->isNullable());
            return obj;
        }
        case TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            const auto & value_type = map_type.getValueType();
            Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
            obj->set("type", "map");
            obj->set("keyType", deltaTypeToJSON(map_type.getKeyType()));
            obj->set("valueType", deltaTypeToJSON(value_type));
            obj->set("valueContainsNull", value_type->isNullable());
            return obj;
        }
        case TypeIndex::Tuple:
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
            const auto & element_types = tuple_type.getElements();
            const auto & element_names = tuple_type.getElementNames();
            Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
            for (size_t i = 0; i < element_types.size(); ++i)
            {
                Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
                field->set("name", element_names[i]);
                field->set("type", deltaTypeToJSON(element_types[i]));
                field->set("nullable", element_types[i]->isNullable());
                field->set("metadata", Poco::JSON::Object::Ptr(new Poco::JSON::Object));
                fields->add(field);
            }
            Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
            obj->set("type", "struct");
            obj->set("fields", fields);
            return obj;
        }
        default:
            break;
    }

    switch (DeltaLakeMetadata::classifyDeltaPrimitive(type))
    {
        case DeltaPrimitiveType::Boolean:   return String("boolean");
        case DeltaPrimitiveType::Byte:      return String("byte");
        case DeltaPrimitiveType::Short:     return String("short");
        case DeltaPrimitiveType::Integer:   return String("integer");
        case DeltaPrimitiveType::Long:      return String("long");
        case DeltaPrimitiveType::Float:     return String("float");
        case DeltaPrimitiveType::Double:    return String("double");
        case DeltaPrimitiveType::String:    return String("string");
        case DeltaPrimitiveType::Date:      return String("date");
        case DeltaPrimitiveType::Timestamp: return String("timestamp");
        case DeltaPrimitiveType::Decimal:
            return String(fmt::format("decimal({},{})", getDecimalPrecision(*type), getDecimalScale(*type)));
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled DeltaPrimitiveType for `{}`", type->getName());
}

/// Build the Delta `StructType.fields` array (used for catalog registration).
Poco::JSON::Array::Ptr buildDeltaSchemaFields(const NamesAndTypesList & schema)
{
    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    for (const auto & column : schema)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set("name", column.name);
        field->set("type", deltaTypeToJSON(column.type));
        field->set("nullable", column.type->isNullable());
        field->set("metadata", Poco::JSON::Object::Ptr(new Poco::JSON::Object));
        fields->add(field);
    }
    return fields;
}

}

void registerDeltaTableInCatalog(
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration_ptr,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context,
    const ColumnsDescription & columns,
    bool created_fresh,
    const StorageID & table_id)
{
    /// Register the full URI (`s3://…`, `file://…`) the kernel reads, not the scheme-less `getRawPath().path`.
    const auto location = getKernelHelper(configuration_ptr, object_storage)->getTableLocation();

    /// Register the schema on storage: the just-committed `columns` for a fresh table, else the existing snapshot schema.
    const auto registration_schema = created_fresh
        ? columns.getAllPhysical()
        : DeltaLakeMetadataDeltaKernel::create(object_storage, configuration)->getTableSchema(local_context);

    Poco::JSON::Object::Ptr metadata_content = new Poco::JSON::Object;
    metadata_content->set("location", location);
    metadata_content->set("fields", buildDeltaSchemaFields(registration_schema));

    const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
    try
    {
        catalog->createTable(namespace_name, table_name, location, metadata_content);
    }
    catch (...)
    {
        /// Best-effort roll back the just-written commit 0 so a failed registration does not orphan a fresh `_delta_log`.
        if (created_fresh)
        {
            try
            {
                const auto commit_zero = std::filesystem::path(configuration_ptr->getRawPath().path) / "_delta_log" / "00000000000000000000.json";
                object_storage->removeObjectIfExists(StoredObject(commit_zero));
            }
            catch (...)
            {
                tryLogCurrentException("DeltaLakeCatalogRegistration", "Failed to roll back the initial Delta commit after catalog registration failed");
            }
        }
        throw;
    }
}

}

#endif
