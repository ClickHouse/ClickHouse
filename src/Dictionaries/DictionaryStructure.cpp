#include "DictionaryStructure.h"
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/StringUtils/StringUtils.h>

#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    DictionaryTypedSpecialAttribute makeDictionaryTypedSpecialAttribute(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & default_type)
    {
        const auto name = config.getString(config_prefix + ".name", "");
        const auto expression = config.getString(config_prefix + ".expression", "");

        if (name.empty() && !expression.empty())
            throw Exception{"Element " + config_prefix + ".name is empty", ErrorCodes::BAD_ARGUMENTS};

        const auto type_name = config.getString(config_prefix + ".type", default_type);
        return DictionaryTypedSpecialAttribute{std::move(name), std::move(expression), DataTypeFactory::instance().get(type_name)};
    }

}


AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type)
{
    static const std::unordered_map<std::string, AttributeUnderlyingType> dictionary
    {
        {"UInt8", AttributeUnderlyingType::utUInt8},
        {"UInt16", AttributeUnderlyingType::utUInt16},
        {"UInt32", AttributeUnderlyingType::utUInt32},
        {"UInt64", AttributeUnderlyingType::utUInt64},
        {"UUID", AttributeUnderlyingType::utUInt128},
        {"Int8", AttributeUnderlyingType::utInt8},
        {"Int16", AttributeUnderlyingType::utInt16},
        {"Int32", AttributeUnderlyingType::utInt32},
        {"Int64", AttributeUnderlyingType::utInt64},
        {"Float32", AttributeUnderlyingType::utFloat32},
        {"Float64", AttributeUnderlyingType::utFloat64},
        {"String", AttributeUnderlyingType::utString},
        {"Date", AttributeUnderlyingType::utUInt16},
    };

    const auto it = dictionary.find(type);
    if (it != std::end(dictionary))
        return it->second;

    /// Can contain arbitrary scale and timezone parameters.
    if (type.find("DateTime64") == 0)
        return AttributeUnderlyingType::utUInt64;

    /// Can contain arbitrary timezone as parameter.
    if (type.find("DateTime") == 0)
        return AttributeUnderlyingType::utUInt32;

    if (type.find("Decimal") == 0)
    {
        size_t start = strlen("Decimal");
        if (type.find("32", start) == start)
            return AttributeUnderlyingType::utDecimal32;
        if (type.find("64", start) == start)
            return AttributeUnderlyingType::utDecimal64;
        if (type.find("128", start) == start)
            return AttributeUnderlyingType::utDecimal128;
    }

    // Temporary hack to allow arrays in keys, since they are never retrieved for polygon dictionaries.
    // TODO: This should be fixed by fully supporting arrays in dictionaries.
    if (type.find("Array") == 0)
        return AttributeUnderlyingType::utString;

    throw Exception{"Unknown type " + type, ErrorCodes::UNKNOWN_TYPE};
}


std::string toString(const AttributeUnderlyingType type)
{
    switch (type)
    {
        case AttributeUnderlyingType::utUInt8:
            return "UInt8";
        case AttributeUnderlyingType::utUInt16:
            return "UInt16";
        case AttributeUnderlyingType::utUInt32:
            return "UInt32";
        case AttributeUnderlyingType::utUInt64:
            return "UInt64";
        case AttributeUnderlyingType::utUInt128:
            return "UUID";
        case AttributeUnderlyingType::utInt8:
            return "Int8";
        case AttributeUnderlyingType::utInt16:
            return "Int16";
        case AttributeUnderlyingType::utInt32:
            return "Int32";
        case AttributeUnderlyingType::utInt64:
            return "Int64";
        case AttributeUnderlyingType::utFloat32:
            return "Float32";
        case AttributeUnderlyingType::utFloat64:
            return "Float64";
        case AttributeUnderlyingType::utDecimal32:
            return "Decimal32";
        case AttributeUnderlyingType::utDecimal64:
            return "Decimal64";
        case AttributeUnderlyingType::utDecimal128:
            return "Decimal128";
        case AttributeUnderlyingType::utString:
            return "String";
    }

    throw Exception{"Unknown attribute_type " + toString(static_cast<int>(type)), ErrorCodes::ARGUMENT_OUT_OF_BOUND};
}


DictionarySpecialAttribute::DictionarySpecialAttribute(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : name{config.getString(config_prefix + ".name", "")}, expression{config.getString(config_prefix + ".expression", "")}
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

        const char * range_default_type = "Date";
        if (config.has(config_prefix + ".range_min"))
            range_min.emplace(makeDictionaryTypedSpecialAttribute(config, config_prefix + ".range_min", range_default_type));

        if (config.has(config_prefix + ".range_max"))
            range_max.emplace(makeDictionaryTypedSpecialAttribute(config, config_prefix + ".range_max", range_default_type));

        if (range_min.has_value() != range_max.has_value())
        {
            throw Exception{"Dictionary structure should have both 'range_min' and 'range_max' either specified or not.",
                            ErrorCodes::BAD_ARGUMENTS};
        }

        if (range_min && range_max && !range_min->type->equals(*range_max->type))
        {
            throw Exception{"Dictionary structure 'range_min' and 'range_max' should have same type, "
                            "'range_min' type: "
                                + range_min->type->getName()
                                + ", "
                                  "'range_max' type: "
                                + range_max->type->getName(),
                            ErrorCodes::BAD_ARGUMENTS};
        }

        if (range_min)
        {
            if (!range_min->type->isValueRepresentedByInteger())
                throw Exception{"Dictionary structure type of 'range_min' and 'range_max' should be an integer, Date, DateTime, or Enum."
                                " Actual 'range_min' and 'range_max' type is "
                                    + range_min->type->getName(),
                                ErrorCodes::BAD_ARGUMENTS};
        }

        if (!id->expression.empty() || (range_min && !range_min->expression.empty()) || (range_max && !range_max->expression.empty()))
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
            throw Exception{"Key type at position " + std::to_string(i) + " does not match, expected " + expected_type + ", found "
                                + actual_type,
                            ErrorCodes::TYPE_MISMATCH};
    }
}


std::string DictionaryStructure::getKeyDescription() const
{
    if (id)
        return "UInt64";

    WriteBufferFromOwnString out;

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
        if (key_i.underlying_type == AttributeUnderlyingType::utString)
            return false;

    return true;
}

size_t DictionaryStructure::getKeySize() const
{
    return std::accumulate(std::begin(*key), std::end(*key), size_t{}, [](const auto running_size, const auto & key_i)
    {
        return running_size + key_i.type->getSizeOfValueInMemory();
    });
}


static void checkAttributeKeys(const Poco::Util::AbstractConfiguration::Keys & keys)
{
    static const std::unordered_set<std::string> valid_keys
        = {"name", "type", "expression", "null_value", "hierarchical", "injective", "is_object_id"};

    for (const auto & key : keys)
    {
        if (valid_keys.find(key) == valid_keys.end())
            throw Exception{"Unknown key '" + key + "' inside attribute section", ErrorCodes::BAD_ARGUMENTS};
    }
}


std::vector<DictionaryAttribute> DictionaryStructure::getAttributes(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const bool hierarchy_allowed,
    const bool allow_null_values)
{
    Poco::Util::AbstractConfiguration::Keys config_elems;
    config.keys(config_prefix, config_elems);
    auto has_hierarchy = false;

    std::vector<DictionaryAttribute> res_attributes;

    const FormatSettings format_settings;

    for (const auto & config_elem : config_elems)
    {
        if (!startsWith(config_elem, "attribute"))
            continue;

        const auto prefix = config_prefix + '.' + config_elem + '.';
        Poco::Util::AbstractConfiguration::Keys attribute_keys;
        config.keys(config_prefix + '.' + config_elem, attribute_keys);

        checkAttributeKeys(attribute_keys);

        const auto name = config.getString(prefix + "name");

        /// Don't include range_min and range_max in attributes list, otherwise
        /// columns will be duplicated
        if ((range_min && name == range_min->name) || (range_max && name == range_max->name))
            continue;

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
                if (null_value_string.empty())
                    null_value = type->getDefault();
                else
                {
                    ReadBufferFromString null_value_buffer{null_value_string};
                    auto column_with_null_value = type->createColumn();
                    type->deserializeAsTextEscaped(*column_with_null_value, null_value_buffer, format_settings);
                    null_value = (*column_with_null_value)[0];
                }
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

        res_attributes.emplace_back(
            DictionaryAttribute{name, underlying_type, type, expression, null_value, hierarchical, injective, is_object_id});
    }

    return res_attributes;
}

}
