#include "ComplexKeyCacheDictionary.h"

namespace DB
{
void ComplexKeyCacheDictionary::setAttributeValue(Attribute & attribute, const size_t idx, const Field & value) const
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = value.get<UInt128>();
            break;
        case AttributeUnderlyingType::utInt8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utFloat32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>();
            break;
        case AttributeUnderlyingType::utFloat64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>();
            break;

        case AttributeUnderlyingType::utDecimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = value.get<Decimal32>();
            break;
        case AttributeUnderlyingType::utDecimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = value.get<Decimal64>();
            break;
        case AttributeUnderlyingType::utDecimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = value.get<Decimal128>();
            break;

        case AttributeUnderlyingType::utString:
        {
            const auto & string = value.get<String>();
            auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
            const auto & null_value_ref = std::get<String>(attribute.null_values);

            /// free memory unless it points to a null_value
            if (string_ref.data && string_ref.data != null_value_ref.data())
                string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

            const auto str_size = string.size();
            if (str_size != 0)
            {
                auto * str_ptr = string_arena->alloc(str_size);
                std::copy(string.data(), string.data() + str_size, str_ptr);
                string_ref = StringRef{str_ptr, str_size};
            }
            else
                string_ref = {};

            break;
        }
    }
}

}
