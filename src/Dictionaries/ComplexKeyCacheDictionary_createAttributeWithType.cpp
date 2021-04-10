#include "ComplexKeyCacheDictionary.h"

namespace DB
{
ComplexKeyCacheDictionary::Attribute
ComplexKeyCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}};

    switch (type)
    {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
        attr.null_values = TYPE(null_value.get<NearestFieldType<TYPE>>()); /* NOLINT */ \
        attr.arrays = std::make_unique<ContainerType<TYPE>>(size); /* NOLINT */ \
        bytes_allocated += size * sizeof(TYPE); \
        break;
        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
#undef DISPATCH
        case AttributeUnderlyingType::utString:
            attr.null_values = null_value.get<String>();
            attr.arrays = std::make_unique<ContainerType<StringRef>>(size);
            bytes_allocated += size * sizeof(StringRef);
            if (!string_arena)
                string_arena = std::make_unique<ArenaWithFreeLists>();
            break;
    }

    return attr;
}

}
