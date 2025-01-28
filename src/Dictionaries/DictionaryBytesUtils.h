#pragma once

#include <Common/PODArray_fwd.h>
#include <Common/FieldVisitorByteSize.h>
#include <Dictionaries/HashedDictionaryCollectionTraits.h>

namespace DB
{

template <typename V> bool isFixedSizeValue()
{
    using FieldType = Field::TypeToEnum<NearestFieldType<V>>;

    /// StringRef values are stored in arenas and are accounted separately.
    return std::is_same_v<V, StringRef> || isFixedSizeFieldType(FieldType::value);
}

template <typename C> size_t getAllocatedBytesInFields(const C & c)
{
    if (isFixedSizeValue<typename C::mapped_type>())
        return 0;

    size_t res = 0;
    FieldVisitorAllocatedBytes visitor;

    /// Subtract the size of Field because it is counted
    /// as the size of a Cell in 'getBufferSizeInBytes'.
    for (const auto & [_, value] : c)
        res += visitor(value) - sizeof(Field);
    return res;
}

template <typename T> size_t getAllocatedBytesInFields(const std::vector<T> & c)
{
    if (isFixedSizeValue<T>())
        return 0;

    size_t res = 0;
    FieldVisitorAllocatedBytes visitor;

    /// Subtract the size of Field because it is counted
    /// as the size of a Cell in 'getBufferSizeInBytes'.
    for (const auto & value : c)
        res += visitor(value) - sizeof(Field);
    return res;
}

/// PaddedPODArray cannot contain Field because is not a POD type.
template <typename T> size_t getAllocatedBytesInFields(const PaddedPODArray<T> &) { return 0; }

using HashedDictionaryImpl::getBufferSizeInBytes;
template <typename T> size_t getBufferSizeInBytes(const std::vector<T> & c) { return c.capacity() * sizeof(T); }
template <typename T> size_t getBufferSizeInBytes(const PaddedPODArray<T> & c) { return c.allocated_bytes(); }

template <typename C> size_t getAllocatedBytesInContainer(const C & c)
{
    return sizeof(c) + getBufferSizeInBytes(c) + getAllocatedBytesInFields(c);
}

}
