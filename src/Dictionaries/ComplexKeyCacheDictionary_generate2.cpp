#include <Dictionaries/ComplexKeyCacheDictionary.h>

namespace DB
{
#define DEFINE(TYPE) \
    void ComplexKeyCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        dict_struct.validateKeyTypes(key_types); \
        auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>(attribute, key_columns, out, [&](const size_t row) { return def[row]; }); \
    }

DEFINE(UInt8)
DEFINE(UInt16)
DEFINE(UInt32)
DEFINE(UInt64)
DEFINE(UInt128)
DEFINE(Int8)
DEFINE(Int16)
DEFINE(Int32)
DEFINE(Int64)
DEFINE(Float32)
DEFINE(Float64)
DEFINE(Decimal32)
DEFINE(Decimal64)
DEFINE(Decimal128)

#undef DEFINE
}
