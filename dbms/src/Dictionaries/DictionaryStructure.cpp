#include <Dictionaries/DictionaryStructure.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/IColumn.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>

#include <ext/range.h>
#include <numeric>
#include <unordered_set>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
}


bool isAttributeTypeConvertibleTo(AttributeUnderlyingType from, AttributeUnderlyingType to)
{
    if (from == to)
        return true;

    /** This enum can be somewhat incomplete and the meaning may not coincide with NumberTraits.h.
      * (for example, because integers can not be converted to floats)
      * This is normal for a limited usage scope.
      */
    if (    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::UInt16)
        ||    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::UInt32)
        ||    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::UInt64)
        ||    (from == AttributeUnderlyingType::UInt16 && to == AttributeUnderlyingType::UInt32)
        ||    (from == AttributeUnderlyingType::UInt16 && to == AttributeUnderlyingType::UInt64)
        ||    (from == AttributeUnderlyingType::UInt32 && to == AttributeUnderlyingType::UInt64)
        ||    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::Int16)
        ||    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::Int32)
        ||    (from == AttributeUnderlyingType::UInt8 && to == AttributeUnderlyingType::Int64)
        ||    (from == AttributeUnderlyingType::UInt16 && to == AttributeUnderlyingType::Int32)
        ||    (from == AttributeUnderlyingType::UInt16 && to == AttributeUnderlyingType::Int64)
        ||    (from == AttributeUnderlyingType::UInt32 && to == AttributeUnderlyingType::Int64)

        ||    (from == AttributeUnderlyingType::Int8 && to == AttributeUnderlyingType::Int16)
        ||    (from == AttributeUnderlyingType::Int8 && to == AttributeUnderlyingType::Int32)
        ||    (from == AttributeUnderlyingType::Int8 && to == AttributeUnderlyingType::Int64)
        ||    (from == AttributeUnderlyingType::Int16 && to == AttributeUnderlyingType::Int32)
        ||    (from == AttributeUnderlyingType::Int16 && to == AttributeUnderlyingType::Int64)
        ||    (from == AttributeUnderlyingType::Int32 && to == AttributeUnderlyingType::Int64)

        ||    (from == AttributeUnderlyingType::Float32 && to == AttributeUnderlyingType::Float64))
    {
        return true;
    }

    return false;
}


AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type)
{
    static const std::unordered_map<std::string, AttributeUnderlyingType> dictionary{
        { "UInt8", AttributeUnderlyingType::UInt8 },
        { "UInt16", AttributeUnderlyingType::UInt16 },
        { "UInt32", AttributeUnderlyingType::UInt32 },
        { "UInt64", AttributeUnderlyingType::UInt64 },
        { "UUID", AttributeUnderlyingType::UInt128 },
        { "Int8", AttributeUnderlyingType::Int8 },
        { "Int16", AttributeUnderlyingType::Int16 },
        { "Int32", AttributeUnderlyingType::Int32 },
        { "Int64", AttributeUnderlyingType::Int64 },
        { "Float32", AttributeUnderlyingType::Float32 },
        { "Float64", AttributeUnderlyingType::Float64 },
        { "String", AttributeUnderlyingType::String },
        { "Date", AttributeUnderlyingType::UInt16 },
        { "DateTime", AttributeUnderlyingType::UInt32 },
    };

    const auto it = dictionary.find(type);
    if (it != std::end(dictionary))
        return it->second;

    throw Exception{"Unknown type " + type, ErrorCodes::UNKNOWN_TYPE};
}


std::string toString(const AttributeUnderlyingType type)
{
    switch (type)
    {
        case AttributeUnderlyingType::UInt8: return "UInt8";
        case AttributeUnderlyingType::UInt16: return "UInt16";
        case AttributeUnderlyingType::UInt32: return "UInt32";
        case AttributeUnderlyingType::UInt64: return "UInt64";
        case AttributeUnderlyingType::UInt128: return "UUID";
        case AttributeUnderlyingType::Int8: return "Int8";
        case AttributeUnderlyingType::Int16: return "Int16";
        case AttributeUnderlyingType::Int32: return "Int32";
        case AttributeUnderlyingType::Int64: return "Int64";
        case AttributeUnderlyingType::Float32: return "Float32";
        case AttributeUnderlyingType::Float64: return "Float64";
        case AttributeUnderlyingType::String: return "String";
    }

    throw Exception{"Unknown attribute_type " + toString(static_cast<int>(type)), ErrorCodes::ARGUMENT_OUT_OF_BOUND};
}


DictionarySpecialAttribute::DictionarySpecialAttribute(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : name{config.getString(config_prefix + ".name", "")},
      expression{config.getString(config_prefix + ".expression", "")}
{
    if (name.empty() && !expression.empty())
        throw Exception{"Element " + config_prefix + ".name is empty", ErrorCodes::BAD_ARGUMENTS};
}


DictionaryStructure::DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    const auto has_id = config.has(config_prefix + ".id");
    const auto has_key = config.has(config_prefix + ".key");

    if (has_key && has_id)
        throw Exception{"Only one of 'id' and 'key' should be specified", ErrorCodes::BAD_ARGUMENTS};

    if (has_id)
        id.emplace(config, config_prefix + ".id");
    else if (has_key)
    {
        key.emplace(getAttributes(config, config_prefix + ".key", false, false));
        if (key->empty())
            throw Exception{"Empty 'key' supplied", ErrorCodes::BAD_ARGUMENTS};
    }
    else
        throw Exception{"Dictionary structure should specify either 'id' or 'key'", ErrorCodes::BAD_ARGUMENTS};

    if (id)
    {
        if (id->name.empty())
            throw Exception{"'id' cannot be empty", ErrorCodes::BAD_ARGUMENTS};

        if (config.has(config_prefix + ".range_min"))
            range_min.emplace(config, config_prefix + ".range_min");

        if (config.has(config_prefix + ".range_max"))
            range_max.emplace(config, config_prefix + ".range_max");

        if (!id->expression.empty() ||
            (range_min && !range_min->expression.empty()) ||
            (range_max && !range_max->expression.empty()))
            has_expressions = true;
    }

    attributes = getAttributes(config, config_prefix);
    if (attributes.empty())
        throw Exception{"Dictionary has no attributes defined", ErrorCodes::BAD_ARGUMENTS};
}


void DictionaryStructure::validateKeyTypes(const DataTypes & key_types) const
{
    if (key_types.size() != key->size())
        throw Exception{"Key structure does not match, expected " + getKeyDescription(), ErrorCodes::TYPE_MISMATCH};

    for (const auto i : ext::range(0, key_types.size()))
    {
        const auto & expected_type = (*key)[i].type->getName();
        const auto & actual_type = key_types[i]->getName();

        if (expected_type != actual_type)
            throw Exception{"Key type at position " + std::to_string(i) + " does not match, expected " + expected_type +
                ", found " + actual_type, ErrorCodes::TYPE_MISMATCH};
    }
}


std::string DictionaryStructure::getKeyDescription() const
{
    if (id)
        return "UInt64";

    std::ostringstream out;

    out << '(';

    auto first = true;
    for (const auto & key_i : *key)
    {
        if (!first)
            out << ", ";

        first = false;

        out << key_i.type->getName();
    }

    out << ')';

    return out.str();
}


bool DictionaryStructure::isKeySizeFixed() const
{
    if (!key)
        return true;

    for (const auto & key_i : *key)
        if (key_i.underlying_type == AttributeUnderlyingType::String)
            return false;

    return true;
}

size_t DictionaryStructure::getKeySize() const
{
    return std::accumulate(std::begin(*key), std::end(*key), size_t{},
        [] (const auto running_size, const auto & key_i) {return running_size + key_i.type->getSizeOfValueInMemory(); });
}


static void checkAttributeKeys(const Poco::Util::AbstractConfiguration::Keys & keys)
{
    static const std::unordered_set<std::string> valid_keys =
        { "name", "type", "expression", "null_value", "hierarchical", "injective", "is_object_id" };

    for (const auto & key : keys)
    {
        if (valid_keys.find(key) == valid_keys.end())
            throw Exception{"Unknown key '" + key + "' inside attribute section", ErrorCodes::BAD_ARGUMENTS};
    }
}


std::vector<DictionaryAttribute> DictionaryStructure::getAttributes(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const bool hierarchy_allowed, const bool allow_null_values)
{
    Poco::Util::AbstractConfiguration::Keys config_elems;
    config.keys(config_prefix, config_elems);
    auto has_hierarchy = false;

    std::vector<DictionaryAttribute> res_attributes;

    const FormatSettings format_settings;

    for (const auto & config_elem : config_elems)
    {
        if (!startsWith(config_elem.data(), "attribute"))
            continue;

        const auto prefix = config_prefix + '.' + config_elem + '.';
        Poco::Util::AbstractConfiguration::Keys attribute_keys;
        config.keys(config_prefix + '.' + config_elem, attribute_keys);

        checkAttributeKeys(attribute_keys);

        const auto name = config.getString(prefix + "name");
        const auto type_string = config.getString(prefix + "type");
        const auto type = DataTypeFactory::instance().get(type_string);
        const auto underlying_type = getAttributeUnderlyingType(type_string);

        const auto expression = config.getString(prefix + "expression", "");
        if (!expression.empty())
            has_expressions = true;

        Field null_value;
        if (allow_null_values)
        {
            const auto null_value_string = config.getString(prefix + "null_value");
            try
            {
                ReadBufferFromString null_value_buffer{null_value_string};
                auto column_with_null_value = type->createColumn();
                type->deserializeTextEscaped(*column_with_null_value, null_value_buffer, format_settings);
                null_value = (*column_with_null_value)[0];
            }
            catch (Exception & e)
            {
                e.addMessage("error parsing null_value");
                throw;
            }
        }

        const auto hierarchical = config.getBool(prefix + "hierarchical", false);
        const auto injective = config.getBool(prefix + "injective", false);
        const auto is_object_id = config.getBool(prefix + "is_object_id", false);
        if (name.empty())
            throw Exception{"Properties 'name' and 'type' of an attribute cannot be empty", ErrorCodes::BAD_ARGUMENTS};

        if (has_hierarchy && !hierarchy_allowed)
            throw Exception{"Hierarchy not allowed in '" + prefix, ErrorCodes::BAD_ARGUMENTS};

        if (has_hierarchy && hierarchical)
            throw Exception{"Only one hierarchical attribute supported", ErrorCodes::BAD_ARGUMENTS};

        has_hierarchy = has_hierarchy || hierarchical;

        res_attributes.emplace_back(DictionaryAttribute{
            name, underlying_type, type, expression, null_value, hierarchical, injective, is_object_id
        });
    }

    return res_attributes;
}

}
