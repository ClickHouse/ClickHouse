#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>


#include <Poco/Util/AbstractConfiguration.h>

#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/IExternalLoadable.h>

namespace DB
{

enum class AttributeUnderlyingType
{
    utUInt8,
    utUInt16,
    utUInt32,
    utUInt64,
    utUInt128,
    utInt8,
    utInt16,
    utInt32,
    utInt64,
    utFloat32,
    utFloat64,
    utDecimal32,
    utDecimal64,
    utDecimal128,
    utDecimal256,
    utString
};


AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type);

std::string toString(AttributeUnderlyingType type);

/// Min and max lifetimes for a dictionary or it's entry
using DictionaryLifetime = ExternalLoadableLifetime;

/** Holds the description of a single dictionary attribute:
*    - name, used for lookup into dictionary and source;
*    - type, used in conjunction with DataTypeFactory and getAttributeUnderlyingTypeByname;
*    - nested_type, contains nested type of complex type like Nullable, Array
*    - null_value, used as a default value for non-existent entries in the dictionary,
*        decimal representation for numeric attributes;
*    - hierarchical, whether this attribute defines a hierarchy;
*    - injective, whether the mapping to parent is injective (can be used for optimization of GROUP BY?)
*    - is_object_id, used in mongo dictionary, converts string key to objectid
*/
struct DictionaryAttribute final
{
    const std::string name;
    const AttributeUnderlyingType underlying_type;
    const DataTypePtr type;
    const SerializationPtr serialization;
    const DataTypePtr nested_type;
    const std::string expression;
    const Field null_value;
    const bool hierarchical;
    const bool injective;
    const bool is_object_id;
    const bool is_nullable;
    const bool is_array;
};

template <typename Type>
struct DictionaryAttributeType
{
    using AttributeType = Type;
};

template <typename F>
void callOnDictionaryAttributeType(AttributeUnderlyingType type, F&& func)
{
    switch (type)
    {
        case AttributeUnderlyingType::utUInt8:
            func(DictionaryAttributeType<UInt8>());
            break;
        case AttributeUnderlyingType::utUInt16:
            func(DictionaryAttributeType<UInt16>());
            break;
        case AttributeUnderlyingType::utUInt32:
            func(DictionaryAttributeType<UInt32>());
            break;
        case AttributeUnderlyingType::utUInt64:
            func(DictionaryAttributeType<UInt64>());
            break;
        case AttributeUnderlyingType::utUInt128:
            func(DictionaryAttributeType<UInt128>());
            break;
        case AttributeUnderlyingType::utInt8:
            func(DictionaryAttributeType<Int8>());
            break;
        case AttributeUnderlyingType::utInt16:
            func(DictionaryAttributeType<Int16>());
            break;
        case AttributeUnderlyingType::utInt32:
            func(DictionaryAttributeType<Int32>());
            break;
        case AttributeUnderlyingType::utInt64:
            func(DictionaryAttributeType<Int64>());
            break;
        case AttributeUnderlyingType::utFloat32:
            func(DictionaryAttributeType<Float32>());
            break;
        case AttributeUnderlyingType::utFloat64:
            func(DictionaryAttributeType<Float64>());
            break;
        case AttributeUnderlyingType::utString:
            func(DictionaryAttributeType<String>());
            break;
        case AttributeUnderlyingType::utDecimal32:
            func(DictionaryAttributeType<Decimal32>());
            break;
        case AttributeUnderlyingType::utDecimal64:
            func(DictionaryAttributeType<Decimal64>());
            break;
        case AttributeUnderlyingType::utDecimal128:
            func(DictionaryAttributeType<Decimal128>());
            break;
        case AttributeUnderlyingType::utDecimal256:
            func(DictionaryAttributeType<Decimal256>());
            break;
    }
};

struct DictionarySpecialAttribute final
{
    const std::string name;
    const std::string expression;

    DictionarySpecialAttribute(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

struct DictionaryTypedSpecialAttribute final
{
    const std::string name;
    const std::string expression;
    const DataTypePtr type;
};


/// Name of identifier plus list of attributes
struct DictionaryStructure final
{
    std::optional<DictionarySpecialAttribute> id;
    std::optional<std::vector<DictionaryAttribute>> key;
    std::vector<DictionaryAttribute> attributes;
    std::unordered_map<std::string, size_t> attribute_name_to_index;
    std::optional<DictionaryTypedSpecialAttribute> range_min;
    std::optional<DictionaryTypedSpecialAttribute> range_max;
    std::optional<size_t> hierarchical_attribute_index;

    bool has_expressions = false;
    bool access_to_key_from_attributes = false;

    DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    void validateKeyTypes(const DataTypes & key_types) const;

    const DictionaryAttribute & getAttribute(const std::string & attribute_name) const;
    const DictionaryAttribute & getAttribute(const std::string & attribute_name, const DataTypePtr & type) const;

    Strings getKeysNames() const;
    size_t getKeysSize() const;

    std::string getKeyDescription() const;
    bool isKeySizeFixed() const;

private:
    /// range_min and range_max have to be parsed before this function call
    std::vector<DictionaryAttribute> getAttributes(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        bool complex_key_attributes);
};

}
