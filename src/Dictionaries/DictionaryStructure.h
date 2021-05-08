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
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    String
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
        case AttributeUnderlyingType::UInt8:
            func(DictionaryAttributeType<UInt8>());
            break;
        case AttributeUnderlyingType::UInt16:
            func(DictionaryAttributeType<UInt16>());
            break;
        case AttributeUnderlyingType::UInt32:
            func(DictionaryAttributeType<UInt32>());
            break;
        case AttributeUnderlyingType::UInt64:
            func(DictionaryAttributeType<UInt64>());
            break;
        case AttributeUnderlyingType::UInt128:
            func(DictionaryAttributeType<UInt128>());
            break;
        case AttributeUnderlyingType::UInt256:
            func(DictionaryAttributeType<UInt128>());
            break;
        case AttributeUnderlyingType::Int8:
            func(DictionaryAttributeType<Int8>());
            break;
        case AttributeUnderlyingType::Int16:
            func(DictionaryAttributeType<Int16>());
            break;
        case AttributeUnderlyingType::Int32:
            func(DictionaryAttributeType<Int32>());
            break;
        case AttributeUnderlyingType::Int64:
            func(DictionaryAttributeType<Int64>());
            break;
        case AttributeUnderlyingType::Int128:
            func(DictionaryAttributeType<Int64>());
            break;
        case AttributeUnderlyingType::Int256:
            func(DictionaryAttributeType<Int64>());
            break;
        case AttributeUnderlyingType::Float32:
            func(DictionaryAttributeType<Float32>());
            break;
        case AttributeUnderlyingType::Float64:
            func(DictionaryAttributeType<Float64>());
            break;
        case AttributeUnderlyingType::String:
            func(DictionaryAttributeType<String>());
            break;
        case AttributeUnderlyingType::Decimal32:
            func(DictionaryAttributeType<Decimal32>());
            break;
        case AttributeUnderlyingType::Decimal64:
            func(DictionaryAttributeType<Decimal64>());
            break;
        case AttributeUnderlyingType::Decimal128:
            func(DictionaryAttributeType<Decimal128>());
            break;
        case AttributeUnderlyingType::Decimal256:
            func(DictionaryAttributeType<Decimal256>());
            break;
        case AttributeUnderlyingType::UUID:
            func(DictionaryAttributeType<UUID>());
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
