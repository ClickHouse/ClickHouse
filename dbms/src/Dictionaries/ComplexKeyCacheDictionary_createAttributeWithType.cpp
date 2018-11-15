#include <Dictionaries/ComplexKeyCacheDictionary.h>

namespace DB
{

ComplexKeyCacheDictionary::Attribute ComplexKeyCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8:
            attr.null_values = null_value.get<UInt64>();
            attr.arrays = std::make_unique<ContainerType<UInt8>>(size);
            bytes_allocated += size * sizeof(UInt8);
            break;
        case AttributeUnderlyingType::UInt16:
            attr.null_values = null_value.get<UInt64>();
            attr.arrays = std::make_unique<ContainerType<UInt16>>(size);
            bytes_allocated += size * sizeof(UInt16);
            break;
        case AttributeUnderlyingType::UInt32:
            attr.null_values = null_value.get<UInt64>();
            attr.arrays = std::make_unique<ContainerType<UInt32>>(size);
            bytes_allocated += size * sizeof(UInt32);
            break;
        case AttributeUnderlyingType::UInt64:
            attr.null_values = null_value.get<UInt64>();
            attr.arrays = std::make_unique<ContainerType<UInt64>>(size);
            bytes_allocated += size * sizeof(UInt64);
            break;
        case AttributeUnderlyingType::UInt128:
            attr.null_values = null_value.get<UInt128>();
            attr.arrays = std::make_unique<ContainerType<UInt128>>(size);
            bytes_allocated += size * sizeof(UInt128);
            break;
        case AttributeUnderlyingType::Int8:
            attr.null_values = null_value.get<Int64>();
            attr.arrays = std::make_unique<ContainerType<Int8>>(size);
            bytes_allocated += size * sizeof(Int8);
            break;
        case AttributeUnderlyingType::Int16:
            attr.null_values = null_value.get<Int64>();
            attr.arrays = std::make_unique<ContainerType<Int16>>(size);
            bytes_allocated += size * sizeof(Int16);
            break;
        case AttributeUnderlyingType::Int32:
            attr.null_values = null_value.get<Int64>();
            attr.arrays = std::make_unique<ContainerType<Int32>>(size);
            bytes_allocated += size * sizeof(Int32);
            break;
        case AttributeUnderlyingType::Int64:
            attr.null_values = null_value.get<Int64>();
            attr.arrays = std::make_unique<ContainerType<Int64>>(size);
            bytes_allocated += size * sizeof(Int64);
            break;
        case AttributeUnderlyingType::Float32:
            attr.null_values = null_value.get<Float64>();
            attr.arrays = std::make_unique<ContainerType<Float32>>(size);
            bytes_allocated += size * sizeof(Float32);
            break;
        case AttributeUnderlyingType::Float64:
            attr.null_values = null_value.get<Float64>();
            attr.arrays = std::make_unique<ContainerType<Float64>>(size);
            bytes_allocated += size * sizeof(Float64);
            break;
        case AttributeUnderlyingType::Decimal32:
            attr.null_values = null_value.get<Decimal32>();
            attr.arrays = std::make_unique<ContainerType<Decimal32>>(size);
            bytes_allocated += size * sizeof(Decimal32);
            break;
        case AttributeUnderlyingType::Decimal64:
            attr.null_values = null_value.get<Decimal64>();
            attr.arrays = std::make_unique<ContainerType<Decimal64>>(size);
            bytes_allocated += size * sizeof(Decimal64);
            break;
        case AttributeUnderlyingType::Decimal128:
            attr.null_values = null_value.get<Decimal128>();
            attr.arrays = std::make_unique<ContainerType<Decimal128>>(size);
            bytes_allocated += size * sizeof(Decimal128);
            break;
        case AttributeUnderlyingType::String:
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
