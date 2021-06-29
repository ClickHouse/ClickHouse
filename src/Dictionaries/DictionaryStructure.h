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

#if defined(__GNUC__)
    /// GCC mistakenly warns about the names in enum class.
    #pragma GCC diagnostic ignored "-Wshadow"
#endif


#define FOR_ATTRIBUTE_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256) \
    M(Float32) \
    M(Float64) \
    M(Decimal32) \
    M(Decimal64) \
    M(Decimal128) \
    M(Decimal256) \
    M(UUID) \
    M(String) \
    M(Array) \


namespace DB
{

enum class AttributeUnderlyingType
{
#define M(TYPE) TYPE,
    FOR_ATTRIBUTE_TYPES(M)
#undef M
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
    const SerializationPtr type_serialization;
    const std::string expression;
    const Field null_value;
    const bool hierarchical;
    const bool injective;
    const bool is_object_id;
    const bool is_nullable;
};

template <typename Type>
struct DictionaryAttributeType
{
    using AttributeType = Type;
};

template <typename F>
void callOnDictionaryAttributeType(AttributeUnderlyingType type, F && func)
{
    switch (type)
    {
#define M(TYPE) \
        case AttributeUnderlyingType::TYPE: \
            func(DictionaryAttributeType<TYPE>()); \
            break;
    FOR_ATTRIBUTE_TYPES(M)
#undef M
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
