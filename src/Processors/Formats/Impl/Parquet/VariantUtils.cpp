#include <Processors/Formats/Impl/Parquet/VariantUtils.h>
#include <Processors/Formats/Impl/Parquet/VariantReader.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadHelpers.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

namespace
{

bool tryInsertFlatJSONPathImpl(
    Object & object,
    std::string_view path,
    Field value,
    const FormatSettings * format_settings,
    size_t depth)
{
    if (format_settings)
        VariantReader::checkVariantReadDepth(*format_settings, depth);

    auto [head, tail] = Nested::splitName(path);
    String key = unescapeDotInJSONKey(String(head));

    if (tail.empty())
    {
        auto [_, inserted] = object.try_emplace(std::move(key), std::move(value));
        return inserted;
    }

    auto [it, inserted] = object.try_emplace(key, Object{});
    if (!inserted && it->second.getType() != Field::Types::Object)
        return false;

    return tryInsertFlatJSONPathImpl(it->second.safeGet<Object>(), tail, std::move(value), format_settings, depth + 1);
}

bool tryBuildNestedObjectFieldImpl(
    Field flat_object_field,
    Field & nested_object_field,
    const FormatSettings * format_settings,
    size_t depth)
{
    if (format_settings)
        VariantReader::checkVariantReadDepth(*format_settings, depth);

    if (flat_object_field.getType() != Field::Types::Object)
        return false;

    Object nested_object;
    Object flat_object = std::move(flat_object_field).safeGet<Object>();
    for (auto & [path, value] : flat_object)
    {
        if (!tryInsertFlatJSONPathImpl(nested_object, path, std::move(value), format_settings, depth))
            return false;
    }

    nested_object_field = std::move(nested_object);
    return true;
}

}

bool isNullableStringType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeString *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return typeid_cast<const DataTypeString *>(nullable->getNestedType().get());

    return false;
}

bool isStoredJSONVariantTypeHint(std::string_view hint)
{
    return hint == "JSON" || hint.starts_with("JSON(") || hint == "Nullable(JSON)" || hint.starts_with("Nullable(JSON(");
}

DataTypePtr getVariantTypeHintFromMetadata(std::string_view hint, bool enable_json_parsing)
{
    if (hint.empty())
        return {};

    if (hint == "Dynamic")
        return std::make_shared<DataTypeDynamic>();

    if (hint == "Nullable(Dynamic)")
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDynamic>());

    if (isStoredJSONVariantTypeHint(hint))
    {
        if (enable_json_parsing)
            return DataTypeFactory::instance().get(String(hint));

        if (hint.starts_with("Nullable("))
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

        return std::make_shared<DataTypeString>();
    }

    return {};
}

String getVariantTypeHintForMetadata(const IDataType * type)
{
    if (!type)
        return {};

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
    {
        String nested_hint = getVariantTypeHintForMetadata(nullable->getNestedType().get());
        if (nested_hint.empty())
            return {};

        return "Nullable(" + nested_hint + ")";
    }

    if (typeid_cast<const DataTypeDynamic *>(type))
        return "Dynamic";

    if (const auto * object = typeid_cast<const DataTypeObject *>(type))
    {
        if (object->getSchemaFormat() == DataTypeObject::SchemaFormat::JSON)
            return object->getName();
    }

    return {};
}

DataTypePtr unwrapVariantTypeHint(DataTypePtr type)
{
    while (type)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable->getNestedType();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality->getDictionaryType();
            continue;
        }

        break;
    }

    return type;
}

DataTypePtr makeVariantWrappedTypedValueType(const DataTypePtr & type)
{
    /// Nested `typed_value` lives inside the outer `{ value, typed_value }` wrapper.
    /// For object payloads (`Tuple`) we need `typed_value` itself to be nullable so readers can
    /// distinguish "row used residual `value`" from "row used shredded object fields". Arrays
    /// cannot be wrapped in `Nullable` in ClickHouse types, so keep them as-is.
    DataTypePtr typed_value_type;
    if (typeid_cast<const DataTypeTuple *>(type.get()))
        typed_value_type = std::make_shared<DataTypeNullable>(type);
    else if (typeid_cast<const DataTypeArray *>(type.get()))
        typed_value_type = type;
    else
        typed_value_type = std::make_shared<DataTypeNullable>(type);

    DataTypes elements
    {
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
        typed_value_type,
    };
    Names names {"value", "typed_value"};
    return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeTuple>(elements, names));
}

DataTypePtr makeVariantExactOutputTypeNullable(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (typeid_cast<const DataTypeNullable *>(type.get()))
        return type;

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        DataTypePtr dictionary_type = low_cardinality->getDictionaryType();
        if (!typeid_cast<const DataTypeNullable *>(dictionary_type.get()))
            dictionary_type = makeNullable(dictionary_type);
        return std::make_shared<DataTypeLowCardinality>(dictionary_type);
    }

    return makeNullable(type);
}

DataTypePtr getParquetVariantScalarType(VariantPrimitiveType primitive_type, std::optional<UInt8> scale)
{
    switch (primitive_type)
    {
        case VariantPrimitiveType::BooleanTrue:
        case VariantPrimitiveType::BooleanFalse:
        {
            static const DataTypePtr type = DataTypeFactory::instance().get("Bool");
            return type;
        }
        case VariantPrimitiveType::Int8:
        {
            static const DataTypePtr type = std::make_shared<DataTypeInt8>();
            return type;
        }
        case VariantPrimitiveType::Int16:
        {
            static const DataTypePtr type = std::make_shared<DataTypeInt16>();
            return type;
        }
        case VariantPrimitiveType::Int32:
        {
            static const DataTypePtr type = std::make_shared<DataTypeInt32>();
            return type;
        }
        case VariantPrimitiveType::Int64:
        {
            static const DataTypePtr type = std::make_shared<DataTypeInt64>();
            return type;
        }
        case VariantPrimitiveType::Float:
        {
            static const DataTypePtr type = std::make_shared<DataTypeFloat32>();
            return type;
        }
        case VariantPrimitiveType::Double:
        {
            static const DataTypePtr type = std::make_shared<DataTypeFloat64>();
            return type;
        }
        case VariantPrimitiveType::Date:
        {
            static const DataTypePtr type = std::make_shared<DataTypeDate32>();
            return type;
        }
        case VariantPrimitiveType::TimestampMicros:
        {
            static const DataTypePtr type = std::make_shared<DataTypeDateTime64>(6, "UTC");
            return type;
        }
        case VariantPrimitiveType::TimestampNtzMicros:
        {
            static const DataTypePtr type = std::make_shared<DataTypeDateTime64>(6);
            return type;
        }
        case VariantPrimitiveType::TimeNtzMicros:
        {
            static const DataTypePtr type = std::make_shared<DataTypeTime64>(6);
            return type;
        }
        case VariantPrimitiveType::TimestampNanos:
        {
            static const DataTypePtr type = std::make_shared<DataTypeDateTime64>(9, "UTC");
            return type;
        }
        case VariantPrimitiveType::TimestampNtzNanos:
        {
            static const DataTypePtr type = std::make_shared<DataTypeDateTime64>(9);
            return type;
        }
        case VariantPrimitiveType::String:
        case VariantPrimitiveType::Binary:
        {
            static const DataTypePtr type = std::make_shared<DataTypeString>();
            return type;
        }
        case VariantPrimitiveType::UUID:
        {
            static const DataTypePtr type = std::make_shared<DataTypeUUID>();
            return type;
        }
        case VariantPrimitiveType::Decimal4:
            return std::make_shared<DataTypeDecimal<Decimal32>>(DataTypeDecimal<Decimal32>::maxPrecision(), scale.value_or(0));
        case VariantPrimitiveType::Decimal8:
            return std::make_shared<DataTypeDecimal<Decimal64>>(DataTypeDecimal<Decimal64>::maxPrecision(), scale.value_or(0));
        case VariantPrimitiveType::Decimal16:
            return std::make_shared<DataTypeDecimal<Decimal128>>(DataTypeDecimal<Decimal128>::maxPrecision(), scale.value_or(0));
        case VariantPrimitiveType::Null:
            break;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported scalar `Parquet` `VARIANT` primitive tag: {}", static_cast<UInt32>(primitive_type));
}

DataTypePtr inferVariantMaterializationType(const VariantReader::VariantValue & value, const FormatSettings & format_settings, size_t depth)
{
    VariantReader::checkVariantReadDepth(format_settings, depth);

    if (value.exact_type)
        return value.exact_type;

    if (value.isObjectLike())
        return std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);

    if (value.isArray())
    {
        DataTypePtr common_element_type;
        bool has_null_elements = false;
        bool has_non_null_elements = false;
        bool all_non_null_elements_are_object_like = true;

        for (const auto & element : value.array_elements)
        {
            if (element.isNull())
                continue;

            has_non_null_elements = true;
            if (!element.isObjectLike())
                all_non_null_elements_are_object_like = false;
        }

        for (const auto & element : value.array_elements)
        {
            if (element.isNull())
            {
                has_null_elements = true;
                continue;
            }

            auto element_type = inferVariantMaterializationType(element, format_settings, depth + 1);
            if (!element_type)
            {
                if (all_non_null_elements_are_object_like)
                    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));

                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>());
            }

            if (!common_element_type)
            {
                common_element_type = std::move(element_type);
                continue;
            }

            if (!common_element_type->equals(*element_type))
            {
                if (all_non_null_elements_are_object_like)
                    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));

                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>());
            }
        }

        if (!common_element_type)
        {
            if (has_non_null_elements && all_non_null_elements_are_object_like)
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));

            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>());
        }

        if (has_null_elements)
            common_element_type = makeVariantExactOutputTypeNullable(common_element_type);

        return std::make_shared<DataTypeArray>(common_element_type);
    }

    if (value.isTuple())
    {
        DataTypes element_types;
        Names element_names;
        element_types.reserve(value.tuple_elements.size());
        element_names.reserve(value.tuple_elements.size());

        for (const auto & [name, element] : value.tuple_elements)
        {
            auto element_type = inferVariantMaterializationType(element, format_settings, depth + 1);
            if (!element_type)
                element_type = std::make_shared<DataTypeDynamic>();

            element_types.emplace_back(std::move(element_type));
            element_names.emplace_back(name);
        }

        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }

    return std::make_shared<DataTypeDynamic>();
}

String appendVariantJSONPath(std::string_view parent_path, std::string_view key)
{
    String escaped_key = escapeDotInJSONKey(String(key));
    if (parent_path.empty())
        return escaped_key;

    String result;
    result.reserve(parent_path.size() + 1 + escaped_key.size());
    result.append(parent_path.data(), parent_path.size());
    result.push_back('.');
    result += escaped_key;
    return result;
}

String appendVariantMetadataPath(std::string_view parent_path, std::string_view key)
{
    if (parent_path.empty())
        return String(key);

    String result;
    result.reserve(parent_path.size() + 1 + key.size());
    result.append(parent_path.data(), parent_path.size());
    result.push_back('\x1f');
    result.append(key.data(), key.size());
    return result;
}

bool tryBuildNestedObjectField(
    Field flat_object_field,
    Field & nested_object_field,
    const FormatSettings & format_settings,
    size_t depth)
{
    return tryBuildNestedObjectFieldImpl(std::move(flat_object_field), nested_object_field, &format_settings, depth);
}

std::optional<VariantWrapperLayout> tryGetVariantWrapperLayout(const DataTypePtr & type)
{
    const IDataType * nested_type = type.get();
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(nested_type))
        nested_type = nullable->getNestedType().get();

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type);
    if (!tuple_type || !tuple_type->hasExplicitNames())
        return std::nullopt;

    const size_t num_elements = tuple_type->getElements().size();
    if (num_elements == 0 || num_elements > 2)
        return std::nullopt;

    auto value_pos = tuple_type->tryGetPositionByName("value");
    if (!value_pos.has_value() || !isNullableStringType(tuple_type->getElement(*value_pos).get()))
        return std::nullopt;

    bool value_can_be_null = typeid_cast<const DataTypeNullable *>(tuple_type->getElement(*value_pos).get()) != nullptr;

    std::optional<size_t> typed_value_pos;
    DataTypePtr typed_value_type;
    if (num_elements == 2)
    {
        typed_value_pos = tuple_type->tryGetPositionByName("typed_value");
        if (!typed_value_pos.has_value())
            return std::nullopt;
        typed_value_type = tuple_type->getElement(*typed_value_pos);
    }
    else if (*value_pos != 0)
    {
        return std::nullopt;
    }

    return VariantWrapperLayout
    {
        .value_pos = *value_pos,
        .value_can_be_null = value_can_be_null,
        .typed_value_pos = typed_value_pos,
        .typed_value_type = std::move(typed_value_type),
    };
}

const DataTypeObject * tryGetObjectLikeVariantOutputType(const IDataType * type)
{
    while (type)
    {
        if (const auto * object = typeid_cast<const DataTypeObject *>(type))
            return object;

        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        {
            type = nullable->getNestedType().get();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type))
        {
            type = low_cardinality->getDictionaryType().get();
            continue;
        }

        break;
    }

    return nullptr;
}

bool isObjectLikeVariantOutputType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeObject *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return isObjectLikeVariantOutputType(nullable->getNestedType().get());

    return false;
}

bool isDynamicLikeVariantOutputType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeDynamic *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return isDynamicLikeVariantOutputType(nullable->getNestedType().get());

    return false;
}

bool isComplexVariantExactOutputType(const DataTypePtr & type)
{
    const IDataType * normalized = type.get();
    while (normalized)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(normalized))
        {
            normalized = nullable->getNestedType().get();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(normalized))
        {
            normalized = low_cardinality->getDictionaryType().get();
            continue;
        }

        break;
    }

    if (!normalized)
        return false;

    return typeid_cast<const DataTypeArray *>(normalized)
        || typeid_cast<const DataTypeTuple *>(normalized)
        || typeid_cast<const DataTypeMap *>(normalized)
        || typeid_cast<const DataTypeObject *>(normalized)
        || typeid_cast<const DataTypeDynamic *>(normalized)
        || typeid_cast<const DataTypeVariant *>(normalized);
}

}
