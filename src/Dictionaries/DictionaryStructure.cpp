#include "DictionaryStructure.h"
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionHelpers.h>
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


AttributeUnderlyingType getAttributeUnderlyingType(const DataTypePtr & type)
{
    auto type_index = type->getTypeId();

    switch (type_index)
    {
        case TypeIndex::UInt8:          return AttributeUnderlyingType::utUInt8;
        case TypeIndex::UInt16:         return AttributeUnderlyingType::utUInt16;
        case TypeIndex::UInt32:         return AttributeUnderlyingType::utUInt32;
        case TypeIndex::UInt64:         return AttributeUnderlyingType::utUInt64;
        case TypeIndex::UInt128:        return AttributeUnderlyingType::utUInt128;

        case TypeIndex::Int8:           return AttributeUnderlyingType::utInt8;
        case TypeIndex::Int16:          return AttributeUnderlyingType::utInt16;
        case TypeIndex::Int32:          return AttributeUnderlyingType::utInt32;
        case TypeIndex::Int64:          return AttributeUnderlyingType::utInt64;

        case TypeIndex::Float32:        return AttributeUnderlyingType::utFloat32;
        case TypeIndex::Float64:        return AttributeUnderlyingType::utFloat64;

        case TypeIndex::Decimal32:      return AttributeUnderlyingType::utDecimal32;
        case TypeIndex::Decimal64:      return AttributeUnderlyingType::utDecimal64;
        case TypeIndex::Decimal128:     return AttributeUnderlyingType::utDecimal128;

        case TypeIndex::Date:           return AttributeUnderlyingType::utUInt16;
        case TypeIndex::DateTime:       return AttributeUnderlyingType::utUInt32;
        case TypeIndex::DateTime64:     return AttributeUnderlyingType::utUInt64;

        case TypeIndex::UUID:           return AttributeUnderlyingType::utUInt128;

        case TypeIndex::String:         return AttributeUnderlyingType::utString;

        // Temporary hack to allow arrays in keys, since they are never retrieved for polygon dictionaries.
        // TODO: This should be fixed by fully supporting arrays in dictionaries.
        case TypeIndex::Array:          return AttributeUnderlyingType::utString;

        default: break;
    }

    throw Exception{"Unknown type for dictionary" + type->getName(), ErrorCodes::UNKNOWN_TYPE};
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
    std::string structure_prefix = config_prefix + ".structure";

    const auto has_id = config.has(structure_prefix + ".id");
    const auto has_key = config.has(structure_prefix + ".key");

    if (has_key && has_id)
        throw Exception{"Only one of 'id' and 'key' should be specified", ErrorCodes::BAD_ARGUMENTS};

    if (has_id)
        id.emplace(config, structure_prefix + ".id");
    else if (has_key)
    {
        key.emplace(getAttributes(config, structure_prefix + ".key", true));
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
        if (config.has(structure_prefix + ".range_min"))
            range_min.emplace(makeDictionaryTypedSpecialAttribute(config, structure_prefix + ".range_min", range_default_type));

        if (config.has(structure_prefix + ".range_max"))
            range_max.emplace(makeDictionaryTypedSpecialAttribute(config, structure_prefix + ".range_max", range_default_type));

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

    attributes = getAttributes(config, structure_prefix, false);

    for (size_t i = 0; i < attributes.size(); ++i)
    {
        const auto & attribute_name = attributes[i].name;
        attribute_name_to_index[attribute_name] = i;
    }

    if (attributes.empty())
        throw Exception{"Dictionary has no attributes defined", ErrorCodes::BAD_ARGUMENTS};

    if (config.getBool(config_prefix + ".layout.ip_trie.access_to_key_from_attributes", false))
        access_to_key_from_attributes = true;
}


void DictionaryStructure::validateKeyTypes(const DataTypes & key_types) const
{
    if (key_types.size() != key->size())
        throw Exception{"Key structure does not match, expected " + getKeyDescription(), ErrorCodes::TYPE_MISMATCH};

    for (const auto i : ext::range(0, key_types.size()))
    {
        const auto & expected_type = (*key)[i].type;
        const auto & actual_type = key_types[i];

        if (!areTypesEqual(expected_type, actual_type))
            throw Exception{"Key type at position " + std::to_string(i) + " does not match, expected " + expected_type->getName() + ", found "
                    + actual_type->getName(),
                ErrorCodes::TYPE_MISMATCH};
    }
}

const DictionaryAttribute & DictionaryStructure::getAttribute(const std::string & attribute_name) const
{
    auto it = attribute_name_to_index.find(attribute_name);

    if (it == attribute_name_to_index.end())
    {
        if (!access_to_key_from_attributes)
            throw Exception{"No such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

        for (const auto & key_attribute : *key)
            if (key_attribute.name == attribute_name)
                return key_attribute;

        throw Exception{"No such attribute '" + attribute_name + "' in keys", ErrorCodes::BAD_ARGUMENTS};
    }

    size_t attribute_index = it->second;
    return attributes[attribute_index];
}

const DictionaryAttribute & DictionaryStructure::getAttribute(const std::string & attribute_name, const DataTypePtr & type) const
{
    const auto & attribute = getAttribute(attribute_name);

    if (!areTypesEqual(attribute.type, type))
        throw Exception{"Attribute type does not match, expected " + attribute.type->getName() + ", found " + type->getName(),
            ErrorCodes::TYPE_MISMATCH};

    return attribute;
}

size_t DictionaryStructure::getKeysSize() const
{
    if (id)
        return 1;
    else
        return key->size();
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

Strings DictionaryStructure::getKeysNames() const
{
    if (id)
        return { id->name };

    const auto & key_attributes = *key;

    Strings keys_names;
    keys_names.reserve(key_attributes.size());

    for (const auto & key_attribute : key_attributes)
        keys_names.emplace_back(key_attribute.name);

    return keys_names;
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
    bool complex_key_attributes)
{
    /// If we request complex key attributes they does not support hierarchy and does not allow null values
    const bool hierarchy_allowed = !complex_key_attributes;
    const bool allow_null_values = !complex_key_attributes;

    Poco::Util::AbstractConfiguration::Keys config_elems;
    config.keys(config_prefix, config_elems);
    auto has_hierarchy = false;

    std::unordered_set<String> attribute_names;
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

        auto insert_result = attribute_names.insert(name);
        bool inserted = insert_result.second;

        if (!inserted)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary attributes names must be unique. Attribute name ({}) is not unique",
                name);

        const auto type_string = config.getString(prefix + "type");
        const auto initial_type = DataTypeFactory::instance().get(type_string);
        auto type = initial_type;
        bool is_array = false;
        bool is_nullable = false;

        if (type->isNullable())
        {
            is_nullable = true;
            type = removeNullable(type);
        }

        const auto underlying_type = getAttributeUnderlyingType(type);

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
                {
                    null_value = type->getDefault();
                }
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
                String dictionary_name = config.getString(".dictionary.name", "");
                e.addMessage("While parsing null_value for attribute with name " + name
                    + " in dictionary " + dictionary_name);
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
            name,
            underlying_type,
            initial_type,
            type,
            expression,
            null_value,
            hierarchical,
            injective,
            is_object_id,
            is_nullable,
            is_array});
    }

    return res_attributes;
}

}
