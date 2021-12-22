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


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element {}.name is empty");

    const auto type_name = config.getString(config_prefix + ".type", default_type);
    return DictionaryTypedSpecialAttribute{std::move(name), std::move(expression), DataTypeFactory::instance().get(type_name)};
}

std::optional<AttributeUnderlyingType> maybeGetAttributeUnderlyingType(TypeIndex index)
{
    switch (index) /// Special cases which do not map TypeIndex::T -> AttributeUnderlyingType::T
    {
        case TypeIndex::Date:       return AttributeUnderlyingType::UInt16;
        case TypeIndex::DateTime:   return AttributeUnderlyingType::UInt32;
        case TypeIndex::DateTime64: return AttributeUnderlyingType::UInt64;
        default: break;
    }

    return magic_enum::enum_cast<AttributeUnderlyingType>(static_cast<TypeIndexUnderlying>(index));
}
}


DictionarySpecialAttribute::DictionarySpecialAttribute(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : name{config.getString(config_prefix + ".name", "")}, expression{config.getString(config_prefix + ".expression", "")}
{
    if (name.empty() && !expression.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element {}.name is empty", config_prefix);
}


DictionaryStructure::DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    std::string structure_prefix = config_prefix + ".structure";

    const auto has_id = config.has(structure_prefix + ".id");
    const auto has_key = config.has(structure_prefix + ".key");

    if (has_key && has_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only one of 'id' and 'key' should be specified");

    if (has_id)
        id.emplace(config, structure_prefix + ".id");
    else if (has_key)
    {
        key.emplace(getAttributes(config, structure_prefix + ".key", /*complex_key_attributes =*/ true));
        if (key->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'key' supplied");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary structure should specify either 'id' or 'key'");

    if (id)
    {
        if (id->name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'id' cannot be empty");

        if (!id->expression.empty())
            has_expressions = true;
    }

    parseRangeConfiguration(config, structure_prefix);
    attributes = getAttributes(config, structure_prefix, /*complex_key_attributes =*/ false);

    for (size_t i = 0; i < attributes.size(); ++i)
    {
        const auto & attribute = attributes[i];
        const auto & attribute_name = attribute.name;
        attribute_name_to_index[attribute_name] = i;

        if (attribute.hierarchical)
        {
            if (id && attribute.underlying_type != AttributeUnderlyingType::UInt64)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Hierarchical attribute type for dictionary with simple key must be UInt64. Actual {}",
                    attribute.underlying_type);

            else if (key)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary with complex key does not support hierarchy");

            hierarchical_attribute_index = i;
        }
    }

    if (config.getBool(config_prefix + ".layout.ip_trie.access_to_key_from_attributes", false))
        access_to_key_from_attributes = true;
}


void DictionaryStructure::validateKeyTypes(const DataTypes & key_types) const
{
    if (key_types.size() != key->size())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Key structure does not match, expected {}", getKeyDescription());

    for (size_t i = 0; i < key_types.size(); ++i)
    {
        const auto & expected_type = (*key)[i].type;
        const auto & actual_type = key_types[i];

        if (!areTypesEqual(expected_type, actual_type))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Key type at position {} does not match, expected {}, found {}",
            std::to_string(i),
            expected_type->getName(),
            actual_type->getName());
    }
}

const DictionaryAttribute & DictionaryStructure::getAttribute(const std::string & attribute_name) const
{
    auto it = attribute_name_to_index.find(attribute_name);

    if (it == attribute_name_to_index.end())
    {
        if (!access_to_key_from_attributes)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such attribute '{}'", attribute_name);

        for (const auto & key_attribute : *key)
            if (key_attribute.name == attribute_name)
                return key_attribute;

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such attribute '{}' in keys", attribute_name);
    }

    size_t attribute_index = it->second;
    return attributes[attribute_index];
}

const DictionaryAttribute & DictionaryStructure::getAttribute(const std::string & attribute_name, const DataTypePtr & type) const
{
    const auto & attribute = getAttribute(attribute_name);

    if (!areTypesEqual(attribute.type, type))
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Attribute type does not match, expected {}, found {}",
            attribute.type->getName(),
            type->getName());

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
        if (key_i.underlying_type == AttributeUnderlyingType::String)
            return false;

    return true;
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown key '{}' inside attribute section", key);
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Dictionary attributes names must be unique. Attribute name {} is not unique",
                name);

        const auto type_string = config.getString(prefix + "type");
        const auto initial_type = DataTypeFactory::instance().get(type_string);
        const auto initial_type_serialization = initial_type->getDefaultSerialization();
        bool is_nullable = initial_type->isNullable();

        auto non_nullable_type = removeNullable(initial_type);

        const auto underlying_type_opt = maybeGetAttributeUnderlyingType(non_nullable_type->getTypeId());

        if (!underlying_type_opt)
            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                "Unknown type {} for dictionary attribute", non_nullable_type->getName());

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
                    null_value = initial_type->getDefault();
                }
                else
                {
                    ReadBufferFromString null_value_buffer{null_value_string};
                    auto column_with_null_value = initial_type->createColumn();
                    initial_type_serialization->deserializeWholeText(*column_with_null_value, null_value_buffer, format_settings);
                    null_value = (*column_with_null_value)[0];
                }
            }
            catch (Exception & e)
            {
                String dictionary_name = config.getString(".dictionary.name", "");
                e.addMessage(fmt::format("While parsing null_value for attribute with name {} in dictionary {}", name, dictionary_name));
                throw;
            }
        }

        const auto hierarchical = config.getBool(prefix + "hierarchical", false);
        const auto injective = config.getBool(prefix + "injective", false);
        const auto is_object_id = config.getBool(prefix + "is_object_id", false);
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Properties 'name' and 'type' of an attribute cannot be empty");

        if (has_hierarchy && !hierarchy_allowed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hierarchy not allowed in '{}'", prefix);

        if (has_hierarchy && hierarchical)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only one hierarchical attribute supported");

        has_hierarchy = has_hierarchy || hierarchical;

        res_attributes.emplace_back(DictionaryAttribute{
            name,
            *underlying_type_opt,
            initial_type,
            initial_type_serialization,
            expression,
            null_value,
            hierarchical,
            injective,
            is_object_id,
            is_nullable});
    }

    return res_attributes;
}

void DictionaryStructure::parseRangeConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & structure_prefix)
{
    const char * range_default_type = "Date";
    if (config.has(structure_prefix + ".range_min"))
        range_min.emplace(makeDictionaryTypedSpecialAttribute(config, structure_prefix + ".range_min", range_default_type));

    if (config.has(structure_prefix + ".range_max"))
        range_max.emplace(makeDictionaryTypedSpecialAttribute(config, structure_prefix + ".range_max", range_default_type));

    if (range_min.has_value() != range_max.has_value())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Dictionary structure should have both 'range_min' and 'range_max' either specified or not.");
    }

    if (range_min && range_max && !range_min->type->equals(*range_max->type))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Dictionary structure 'range_min' and 'range_max' should have same type, "
            "'range_min' type: {},"
            "'range_max' type: {}",
            range_min->type->getName(),
            range_max->type->getName());
    }

    if (range_min)
    {
        if (!range_min->type->isValueRepresentedByInteger())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Dictionary structure type of 'range_min' and 'range_max' should be an integer, Date, DateTime, or Enum."
                " Actual 'range_min' and 'range_max' type is {}",
                range_min->type->getName());
    }

    if ((range_min && !range_min->expression.empty()) || (range_max && !range_max->expression.empty()))
        has_expressions = true;
}

}
