#include "CacheDictionary.h"
#include "CacheDictionary.inc.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, ResultArrayType<TYPE> & out) const\
{\
    auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, ids, out, [&] (const size_t) { return def; });\
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
DECLARE(Decimal32)
DECLARE(Decimal64)
DECLARE(Decimal128)
#undef DECLARE

}
