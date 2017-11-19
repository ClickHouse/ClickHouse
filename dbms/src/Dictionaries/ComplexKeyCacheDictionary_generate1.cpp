#include "ComplexKeyCacheDictionary.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

#define DECLARE(TYPE)                                                                                                                   \
    void ComplexKeyCacheDictionary::get##TYPE(                                                                                          \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<TYPE> & out) const \
    {                                                                                                                                   \
        dict_struct.validateKeyTypes(key_types);                                                                                        \
                                                                                                                                        \
        auto & attribute = getAttribute(attribute_name);                                                                                \
        if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))                                               \
            throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),            \
                ErrorCodes::TYPE_MISMATCH};                                                                                             \
                                                                                                                                        \
        const auto null_value = std::get<TYPE>(attribute.null_values);                                                                  \
                                                                                                                                        \
        getItemsNumber<TYPE>(attribute, key_columns, out, [&](const size_t) { return null_value; });                                    \
    }
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE
}
