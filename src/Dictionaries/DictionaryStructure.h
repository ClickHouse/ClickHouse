#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/IExternalLoadable.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <map>
#include <optional>
#include <string>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

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
    utString
};


AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type);

std::string toString(const AttributeUnderlyingType type);

/// Implicit conversions in dictGet functions is disabled.
inline void checkAttributeType(const std::string & dict_name, const std::string & attribute_name,
                               AttributeUnderlyingType attribute_type, AttributeUnderlyingType to)
{
    if (attribute_type != to)
        throw Exception{dict_name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute_type)
            + ", expected " + toString(to), ErrorCodes::TYPE_MISMATCH};
}

/// Min and max lifetimes for a dictionary or it's entry
using DictionaryLifetime = ExternalLoadableLifetime;


/** Holds the description of a single dictionary attribute:
*    - name, used for lookup into dictionary and source;
*    - type, used in conjunction with DataTypeFactory and getAttributeUnderlyingTypeByname;
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
    const std::string expression;
    const Field null_value;
    const bool hierarchical;
    const bool injective;
    const bool is_object_id;
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
    std::optional<DictionaryTypedSpecialAttribute> range_min;
    std::optional<DictionaryTypedSpecialAttribute> range_max;
    bool has_expressions = false;

    DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    void validateKeyTypes(const DataTypes & key_types) const;
    std::string getKeyDescription() const;
    bool isKeySizeFixed() const;
    size_t getKeySize() const;

private:
    /// range_min and range_max have to be parsed before this function call
    std::vector<DictionaryAttribute> getAttributes(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const bool hierarchy_allowed = true,
        const bool allow_null_values = true);
};

}
