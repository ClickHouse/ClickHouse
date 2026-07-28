#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeCatalogRegistration.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
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

#include <optional>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <fmt/format.h>

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
    const std::optional<ColumnsDescription> & columns,
    bool created_fresh,
    const StorageID & table_id)
{
    auto kernel_helper = getKernelHelper(configuration_ptr, object_storage);
    /// Register the full URI (`s3://…`, `file://…`) the kernel reads, not the scheme-less `getRawPath().path`.
    const auto location = kernel_helper->getTableLocation();

    /// Schema to register: the just-committed `columns` for a fresh table (always present in that case); for
    /// an attach, the exact Delta schema of the latest snapshot read straight from the `_delta_log` (ignoring
    /// read-time snapshot/CDF settings), so types like `binary` / `timestamp_ntz` are not collapsed.
    Poco::JSON::Array::Ptr fields;
    if (created_fresh)
        fields = buildDeltaSchemaFields(columns->getAllPhysical());
    else
    {
        auto snapshot = std::make_shared<DeltaLake::TableSnapshot>(
            /* version */ std::nullopt, kernel_helper, object_storage, getLogger("DeltaLakeCatalogRegistration"));
        fields = snapshot->getRawDeltaSchemaFields();
    }

    Poco::JSON::Object::Ptr metadata_content = new Poco::JSON::Object;
    metadata_content->set("location", location);
    metadata_content->set("fields", fields);

    const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());

    /// Do not roll back commit 0 on failure: a generic catalog error is ambiguous (a racing server may have already registered our `_delta_log`), so we keep the log and surface the error rather than risk corrupting that entry.
    catalog->createTable(namespace_name, table_name, location, metadata_content);
}

}

#endif
