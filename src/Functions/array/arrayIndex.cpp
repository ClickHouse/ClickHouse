#include <Functions/array/arrayIndex.h>

#include <algorithm>
#include <cstring>

namespace DB
{

namespace ArrayIndexImpl
{
namespace
{
constexpr size_t NO_MATCH = static_cast<size_t>(-1);

template <typename T, size_t N>
ALWAYS_INLINE bool hasInBlock(const T * data, T value)
{
    unsigned found = 0;

    for (size_t j = 0; j < N; ++j)
        found |= static_cast<unsigned>(data[j] == value);

    return found != 0;
}

template <typename T, size_t N>
ALWAYS_INLINE size_t findFirstIndexInBlock(const T * data, T value)
{
    size_t found = N;

    for (size_t j = 0; j < N; ++j)
    {
        const size_t candidate = data[j] == value ? j : N;
        found = std::min(found, candidate);
    }

    return found;
}

template <SupportedNumeric T>
ALWAYS_INLINE bool findNumericHasInternal(const T * data, size_t size, T value)
{
    if constexpr (sizeof(T) == 1 && std::is_integral_v<T>)
    {
        return std::memchr(data, static_cast<unsigned char>(value), size) != nullptr;
    }
    else
    {
        constexpr size_t block_size = 64 / sizeof(T);
        size_t i = 0;

        for (; size - i >= block_size; i += block_size)
        {
            if (hasInBlock<T, block_size>(data + i, value))
                return true;
        }

        for (; i < size; ++i)
        {
            if (data[i] == value)
                return true;
        }

        return false;
    }
}

template <SupportedNumeric T>
ALWAYS_INLINE bool findNumericHas(const T * data, size_t size, T value)
{
    constexpr size_t prefix_size = 8;

    if constexpr (sizeof(T) == 2 || sizeof(T) == 4)
    {
        constexpr size_t block_size = 64 / sizeof(T);
        if (size >= block_size && size < prefix_size + block_size)
        {
            if (data[0] == value)
                return true;
            return findNumericHasInternal(data, size, value);
        }
    }

    const size_t actual_prefix_size = std::min(size, prefix_size);
    for (size_t i = 0; i < actual_prefix_size; ++i)
        if (data[i] == value)
            return true;

    if (actual_prefix_size == size)
        return false;

    return findNumericHasInternal(data + actual_prefix_size, size - actual_prefix_size, value);
}

template <SupportedNumeric T>
ALWAYS_INLINE size_t findNumericIndexOfInternal(const T * data, size_t size, T value)
{
    if constexpr (sizeof(T) == 1 && std::is_integral_v<T>)
    {
        const auto * found = static_cast<const T *>(std::memchr(data, static_cast<unsigned char>(value), size));
        return found ? static_cast<size_t>(found - data) : NO_MATCH;
    }
    else
    {
        constexpr size_t block_size = 64 / sizeof(T);
        constexpr size_t direct_index_limit = 1024 / sizeof(T);
        size_t i = 0;

        if (size <= direct_index_limit)
        {
            for (; size - i >= block_size; i += block_size)
            {
                const size_t found = findFirstIndexInBlock<T, block_size>(data + i, value);
                if (found != block_size)
                    return i + found;
            }
        }
        else
        {
            for (; size - i >= block_size; i += block_size)
            {
                if (!hasInBlock<T, block_size>(data + i, value))
                    continue;

                for (size_t j = 0; j < block_size; ++j)
                    if (data[i + j] == value)
                        return i + j;
            }
        }

        for (; i < size; ++i)
        {
            if (data[i] == value)
                return i;
        }

        return NO_MATCH;
    }
}

template <SupportedNumeric T>
ALWAYS_INLINE size_t findNumericIndexOf(const T * data, size_t size, T value)
{
    const size_t prefix_size = std::min(size, size_t(8));
    for (size_t i = 0; i < prefix_size; ++i)
        if (data[i] == value)
            return i;

    if (prefix_size == size)
        return NO_MATCH;

    const size_t found = findNumericIndexOfInternal(data + prefix_size, size - prefix_size, value);
    return found == NO_MATCH ? NO_MATCH : prefix_size + found;
}

template <SupportedNumeric T>
ALWAYS_INLINE size_t findNumericScalarIndexOf(const T * data, size_t size, T value)
{
    for (size_t i = 0; i < size; ++i)
        if (data[i] == value)
            return i;

    return NO_MATCH;
}
}

template <SupportedNumeric T>
void findNumericHasBatch(
    const T * __restrict data,
    const ColumnArray::Offset * __restrict offsets,
    UInt8 * __restrict result,
    size_t rows,
    T value)
{
    constexpr size_t min_array_size = getOptimizedSearchMinSize<T, false>();

    ColumnArray::Offset previous_offset = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        const ColumnArray::Offset current_offset = offsets[row];
        const size_t array_size = current_offset - previous_offset;
        const T * __restrict row_data = data + previous_offset;

        if (array_size < min_array_size)
        {
            UInt8 found = 0;
            for (size_t i = 0; i < array_size; ++i)
            {
                if (row_data[i] == value)
                {
                    found = 1;
                    break;
                }
            }
            result[row] = found;
        }
        else
        {
            result[row] = findNumericHas(row_data, array_size, value);
        }

        previous_offset = current_offset;
    }
}

template <SupportedNumeric T>
void findNumericIndexOfBatch(
    const T * __restrict data,
    const ColumnArray::Offset * __restrict offsets,
    UInt64 * __restrict result,
    size_t rows,
    size_t min_array_size,
    T value)
{
    ColumnArray::Offset previous_offset = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        const ColumnArray::Offset current_offset = offsets[row];
        const size_t array_size = current_offset - previous_offset;
        const T * __restrict row_data = data + previous_offset;

        const size_t found = array_size < min_array_size
            ? findNumericScalarIndexOf(row_data, array_size, value)
            : findNumericIndexOf(row_data, array_size, value);
        result[row] = found == NO_MATCH ? 0 : static_cast<UInt64>(found + 1);

        previous_offset = current_offset;
    }
}

#define INSTANTIATE(T) \
    template void findNumericHasBatch<T>( \
        const T * data, const ColumnArray::Offset * offsets, UInt8 * result, size_t rows, T value); \
    template void findNumericIndexOfBatch<T>( \
        const T * data, const ColumnArray::Offset * offsets, UInt64 * result, size_t rows, size_t min_array_size, T value);

INSTANTIATE(Int8)
INSTANTIATE(UInt8)
INSTANTIATE(Int16)
INSTANTIATE(UInt16)
INSTANTIATE(Int32)
INSTANTIATE(UInt32)
INSTANTIATE(Int64)
INSTANTIATE(UInt64)
INSTANTIATE(Float32)
INSTANTIATE(Float64)

#undef INSTANTIATE
}

}
