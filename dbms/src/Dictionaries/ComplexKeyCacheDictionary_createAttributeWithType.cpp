#include <Dictionaries/ComplexKeyCacheDictionary.h>

namespace DB
{

ComplexKeyCacheDictionary::Attribute ComplexKeyCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8:
            std::get<UInt8>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt8>>(attr.arrays) = std::make_unique<ContainerType<UInt8>>(size);
            bytes_allocated += size * sizeof(UInt8);
            break;
        case AttributeUnderlyingType::UInt16:
            std::get<UInt16>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt16>>(attr.arrays) = std::make_unique<ContainerType<UInt16>>(size);
            bytes_allocated += size * sizeof(UInt16);
            break;
        case AttributeUnderlyingType::UInt32:
            std::get<UInt32>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt32>>(attr.arrays) = std::make_unique<ContainerType<UInt32>>(size);
            bytes_allocated += size * sizeof(UInt32);
            break;
        case AttributeUnderlyingType::UInt64:
            std::get<UInt64>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt64>>(attr.arrays) = std::make_unique<ContainerType<UInt64>>(size);
            bytes_allocated += size * sizeof(UInt64);
            break;
        case AttributeUnderlyingType::UInt128:
            std::get<UInt128>(attr.null_values) = null_value.get<UInt128>();
            std::get<ContainerPtrType<UInt128>>(attr.arrays) = std::make_unique<ContainerType<UInt128>>(size);
            bytes_allocated += size * sizeof(UInt128);
            break;
        case AttributeUnderlyingType::Int8:
            std::get<Int8>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int8>>(attr.arrays) = std::make_unique<ContainerType<Int8>>(size);
            bytes_allocated += size * sizeof(Int8);
            break;
        case AttributeUnderlyingType::Int16:
            std::get<Int16>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int16>>(attr.arrays) = std::make_unique<ContainerType<Int16>>(size);
            bytes_allocated += size * sizeof(Int16);
            break;
        case AttributeUnderlyingType::Int32:
            std::get<Int32>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int32>>(attr.arrays) = std::make_unique<ContainerType<Int32>>(size);
            bytes_allocated += size * sizeof(Int32);
            break;
        case AttributeUnderlyingType::Int64:
            std::get<Int64>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int64>>(attr.arrays) = std::make_unique<ContainerType<Int64>>(size);
            bytes_allocated += size * sizeof(Int64);
            break;
        case AttributeUnderlyingType::Float32:
            std::get<Float32>(attr.null_values) = null_value.get<Float64>();
            std::get<ContainerPtrType<Float32>>(attr.arrays) = std::make_unique<ContainerType<Float32>>(size);
            bytes_allocated += size * sizeof(Float32);
            break;
        case AttributeUnderlyingType::Float64:
            std::get<Float64>(attr.null_values) = null_value.get<Float64>();
            std::get<ContainerPtrType<Float64>>(attr.arrays) = std::make_unique<ContainerType<Float64>>(size);
            bytes_allocated += size * sizeof(Float64);
            break;
        case AttributeUnderlyingType::String:
            std::get<String>(attr.null_values) = null_value.get<String>();
            std::get<ContainerPtrType<StringRef>>(attr.arrays) = std::make_unique<ContainerType<StringRef>>(size);
            bytes_allocated += size * sizeof(StringRef);
            if (!string_arena)
                string_arena = std::make_unique<ArenaWithFreeLists>();
            break;
    }

    return attr;
}

}
