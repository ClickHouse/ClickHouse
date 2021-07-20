#include "ComplexKeyCacheDictionary.h"

namespace DB
{
void ComplexKeyCacheDictionary::setDefaultAttributeValue(Attribute & attribute, const size_t idx) const
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = std::get<UInt128>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utFloat32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utFloat64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values);
            break;

        case AttributeUnderlyingType::utDecimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = std::get<Decimal32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utDecimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = std::get<Decimal64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utDecimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = std::get<Decimal128>(attribute.null_values);
            break;

        case AttributeUnderlyingType::utString:
        {
            const auto & null_value_ref = std::get<String>(attribute.null_values);
            auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];

            if (string_ref.data != null_value_ref.data())
            {
                if (string_ref.data)
                    string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

                string_ref = StringRef{null_value_ref};
            }

            break;
        }
    }
}

}
