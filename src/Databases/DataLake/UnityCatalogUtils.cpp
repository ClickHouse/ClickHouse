#include <Databases/DataLake/UnityCatalogUtils.h>

#if USE_PARQUET

#include <sstream>
#include <Common/Exception.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake
{

/// Delta primitive type name (see `DeltaLakeMetadata::getSimpleTypeByName`) -> Unity `ColumnTypeName`.
static std::string deltaPrimitiveToUnityTypeName(const std::string & delta_type)
{
    if (delta_type == "boolean") return "BOOLEAN";
    if (delta_type == "byte")    return "BYTE";
    if (delta_type == "short")   return "SHORT";
    if (delta_type == "integer") return "INT";
    if (delta_type == "long")    return "LONG";
    if (delta_type == "float")   return "FLOAT";
    if (delta_type == "double")  return "DOUBLE";
    if (delta_type == "date")    return "DATE";
    if (delta_type == "timestamp")     return "TIMESTAMP";
    if (delta_type == "timestamp_ntz") return "TIMESTAMP_NTZ";
    if (delta_type == "string")  return "STRING";
    if (delta_type == "binary")  return "BINARY";
    if (delta_type.starts_with("decimal(")) return "DECIMAL";
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot map Delta type `{}` to a Unity column type", delta_type);
}

Poco::JSON::Array::Ptr buildUnityColumnsFromDeltaSchema(const Poco::JSON::Array::Ptr & fields)
{
    Poco::JSON::Array::Ptr columns = new Poco::JSON::Array;
    for (size_t i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(static_cast<int>(i));
        const String name = field->getValue<String>("name");
        const bool nullable = field->getValue<bool>("nullable");
        auto type_var = field->get("type");

        Poco::JSON::Object::Ptr column = new Poco::JSON::Object;
        column->set("name", name);
        column->set("nullable", nullable);
        column->set("position", static_cast<int>(i));

        int precision = 0;
        int scale = 0;
        String type_name;
        String type_text;
        String type_json;

        if (type_var.isString())
        {
            const String & delta_type = type_var.extract<String>();
            type_text = delta_type;
            type_json = '"' + delta_type + '"';
            type_name = deltaPrimitiveToUnityTypeName(delta_type);
            if (type_name == "DECIMAL")
            {
                const auto decimal_type = DB::DeltaLakeMetadata::getSimpleTypeByName(delta_type);
                precision = static_cast<int>(DB::getDecimalPrecision(*decimal_type));
                scale = static_cast<int>(DB::getDecimalScale(*decimal_type));
            }
        }
        else
        {
            const auto & descriptor = type_var.extract<Poco::JSON::Object::Ptr>();
            const String kind = descriptor->getValue<String>("type");
            if (kind == "array")       type_name = "ARRAY";
            else if (kind == "map")    type_name = "MAP";
            else if (kind == "struct") type_name = "STRUCT";
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected complex Delta type `{}`", kind);
            type_text = kind;

            /// Wrap so the read path's `getFieldType(parsed, "type")` sees the descriptor under `type`.
            Poco::JSON::Object::Ptr wrapper = new Poco::JSON::Object;
            wrapper->set("type", descriptor);
            std::ostringstream oss;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            wrapper->stringify(oss);
            type_json = oss.str();
        }

        column->set("type_name", type_name);
        column->set("type_text", type_text);
        column->set("type_json", type_json);
        column->set("type_precision", precision);
        column->set("type_scale", scale);
        columns->add(column);
    }
    return columns;
}

Poco::JSON::Object::Ptr buildUnityCreateTableBody(
    const String & catalog_name,
    const String & schema_name,
    const String & table_name,
    const String & storage_location,
    Poco::JSON::Array::Ptr columns)
{
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object;
    body->set("name", table_name);
    body->set("catalog_name", catalog_name);
    body->set("schema_name", schema_name);
    body->set("table_type", "EXTERNAL");
    body->set("data_source_format", "DELTA");
    body->set("storage_location", storage_location);
    body->set("columns", columns);
    body->set("properties", Poco::JSON::Object::Ptr(new Poco::JSON::Object));
    return body;
}

}

#endif
