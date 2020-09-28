#include <Dictionaries/CacheDictionary.h>
#include <Dictionaries/CacheDictionary.inc.h>

namespace DB
{
#define DEFINE(TYPE) \
    void CacheDictionary::get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) \
        const \
    { \
        auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
        const auto null_value = std::get<TYPE>(attribute.null_value); \
        getItemsNumberImpl<TYPE, TYPE>(attribute, ids, out, [&](const size_t) { return null_value; }); \
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
