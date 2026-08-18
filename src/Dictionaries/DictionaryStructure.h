#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <Poco/Util/AbstractConfiguration.h>

#include <base/EnumReflection.h>

#include <Core/Field.h>
#include <Core/TypeId.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{
using TypeIndexUnderlying = magic_enum::underlying_type_t<TypeIndex>;

// We need to be able to map TypeIndex -> AttributeUnderlyingType and AttributeUnderlyingType -> real type
// The first can be done by defining AttributeUnderlyingType enum values to TypeIndex values and then performing
// a enum_cast.
// The second can be achieved by using TypeIndexToType
#define map_item(__T) __T = static_cast<TypeIndexUnderlying>(TypeIndex::__T)

enum class AttributeUnderlyingType : TypeIndexUnderlying
{
    map_item(Int8), map_item(Int16), map_item(Int32), map_item(Int64), map_item(Int128), map_item(Int256),
    map_item(UInt8), map_item(UInt16), map_item(UInt32), map_item(UInt64), map_item(UInt128), map_item(UInt256),
    map_item(Float32), map_item(Float64),
    map_item(Decimal32), map_item(Decimal64), map_item(Decimal128), map_item(Decimal256),
    map_item(DateTime64),

    map_item(UUID), map_item(String), map_item(Array),

    map_item(IPv4), map_item(IPv6)
};

#undef map_item

/// Min and max lifetimes for a dictionary or its entry
using DictionaryLifetime = ExternalLoadableLifetime;

/** Holds the description of a single dictionary attribute:
*    - name, used for lookup into dictionary and source;
*    - type, used in conjunction with DataTypeFactory and getAttributeUnderlyingTypeByname;
*    - nested_type, contains nested type of complex type like Nullable, Array
*    - null_value, used as a default value for non-existent entries in the dictionary,
*        decimal representation for numeric attributes;
*    - hierarchical, whether this attribute defines a hierarchy;
*    - injective, whether the mapping to parent is injective (can be used for optimization of GROUP BY?);
*    - is_object_id, used in mongo dictionary, converts string key to objectid;
*    - is_nullable, is attribute nullable;
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
    const bool bidirectional;
    const bool injective;
    const bool is_object_id;
    const bool is_nullable;
};

template <AttributeUnderlyingType type>
struct DictionaryAttributeType
{
    /// Converts @c type to it underlying type e.g. AttributeUnderlyingType::UInt8 -> UInt8
    using AttributeType = TypeIndexToType<
        static_cast<TypeIndex>(
            static_cast<TypeIndexUnderlying>(type))>;
};

template <typename F>
constexpr void callOnDictionaryAttributeType(AttributeUnderlyingType type, F && func)
{
    static_for<AttributeUnderlyingType>([type, my_func = std::forward<F>(func)](auto other)
    {
        if (type == other)
            my_func(DictionaryAttributeType<other>{});
    });
}

struct DictionaryTypedSpecialAttribute final
{
    const std::string name;
    const std::string expression;
    const DataTypePtr type;
};


/// Name of identifier plus list of attributes
struct DictionaryStructure final
{
    std::optional<DictionaryTypedSpecialAttribute> id;
    std::optional<std::vector<DictionaryAttribute>> key;
    std::vector<DictionaryAttribute> attributes;
    std::unordered_map<std::string, size_t> attribute_name_to_index;
    std::optional<DictionaryTypedSpecialAttribute> range_min;
    std::optional<DictionaryTypedSpecialAttribute> range_max;
    std::optional<size_t> hierarchical_attribute_index;

    bool has_expressions = false;
    bool access_to_key_from_attributes = false;

    DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    DataTypes getKeyTypes() const;
    void validateKeyTypes(const DataTypes & key_types) const;

    bool hasAttribute(const std::string & attribute_name) const;
    const DictionaryAttribute & getAttribute(const std::string & attribute_name) const;
    const DictionaryAttribute & getAttribute(const std::string & attribute_name, const DataTypePtr & type) const;

    Strings getKeysNames() const;
    size_t getKeysSize() const;

    std::string getKeyDescription() const;

private:
    /// range_min and range_max have to be parsed before this function call
    std::vector<DictionaryAttribute> getAttributes(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        bool complex_key_attributes);

    /// parse range_min and range_max
    void parseRangeConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & structure_prefix);

};

}
