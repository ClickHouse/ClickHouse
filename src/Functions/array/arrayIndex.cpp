#include <Functions/array/arrayIndex.h>

#include <cstring>

namespace DB
{

namespace ArrayIndexImpl
{
namespace
{
template <typename T, size_t N>
ALWAYS_INLINE bool hasInBlock(const T * data, T value)
{
    unsigned found = 0;

#if defined(__clang__)
#pragma clang loop vectorize(enable) interleave(enable)
#endif
    for (size_t j = 0; j < N; ++j)
        found |= static_cast<unsigned>(data[j] == value);

    return found != 0;
}

template <SupportedUnsignedInteger T>
ALWAYS_INLINE bool findUIntHasInternal(const T * data, size_t size, T value)
{
    if constexpr (std::is_same_v<T, UInt8>)
    {
        return std::memchr(data, static_cast<int>(value), size) != nullptr;
    }
    else
    {
        size_t i = 0;

        if (size >= 8)
        {
            if (hasInBlock<T, 8>(data, value))
                return true;
            i = 8;
        }

        for (; size - i >= 16; i += 16)
        {
            if (hasInBlock<T, 16>(data + i, value))
                return true;
        }

        if (size - i >= 8)
        {
            if (hasInBlock<T, 8>(data + i, value))
                return true;
            i += 8;
        }

        for (; i < size; ++i)
        {
            if (data[i] == value)
                return true;
        }

        return false;
    }
}

template <typename T>
ALWAYS_INLINE size_t findScalarPrefix(const T * data, size_t size, T value)
{
    size_t i = 0;

    for (; size - i >= 8; i += 8)
    {
#if defined(__clang__)
#pragma unroll
#endif
        for (size_t j = 0; j < 8; ++j)
        {
            if (data[i + j] == value)
                return i + j;
        }
    }

    if (size - i >= 4)
    {
#if defined(__clang__)
#pragma unroll
#endif
        for (size_t j = 0; j < 4; ++j)
        {
            if (data[i + j] == value)
                return i + j;
        }
        i += 4;
    }

    for (; i < size; ++i)
    {
        if (data[i] == value)
            return i;
    }

    return static_cast<size_t>(-1);
}

template <SupportedUnsignedInteger T>
ALWAYS_INLINE size_t findUIntIndexOfInternal(const T * data, size_t size, T value)
{
    if constexpr (std::is_same_v<T, UInt8>)
    {
        const auto * found = static_cast<const UInt8 *>(std::memchr(data, static_cast<int>(value), size));
        return found ? static_cast<size_t>(found - data) : static_cast<size_t>(-1);
    }
    else
    {
        /// The caller already checked the first eight values inline.
        constexpr size_t scalar_prefix = std::is_same_v<T, UInt64> ? 128 : 64;
        constexpr size_t scalar_continuation = scalar_prefix - 8;
        const size_t scalar_size = size < scalar_continuation ? size : scalar_continuation;
        size_t i = findScalarPrefix(data, scalar_size, value);

        if (i != static_cast<size_t>(-1))
            return i;

        i = scalar_size;
        for (; size - i >= 16; i += 16)
        {
            if (!hasInBlock<T, 16>(data + i, value))
                continue;

#if defined(__clang__)
#pragma unroll
#endif
            for (size_t j = 0; j < 16; ++j)
            {
                if (data[i + j] == value)
                    return i + j;
            }
        }

        for (; i < size; ++i)
        {
            if (data[i] == value)
                return i;
        }

        return static_cast<size_t>(-1);
    }
}
}

template <typename T>
NO_INLINE bool findUIntHas(const T * data, size_t size, T value)
{
    return findUIntHasInternal(data, size, value);
}

template <typename T>
NO_INLINE size_t findUIntIndexOf(const T * data, size_t size, T value)
{
    return findUIntIndexOfInternal(data, size, value);
}

#define INSTANTIATE(T) \
    template bool findUIntHas<T>(const T * data, size_t size, T value); \
    template size_t findUIntIndexOf<T>(const T * data, size_t size, T value);

INSTANTIATE(UInt8)
INSTANTIATE(UInt16)
INSTANTIATE(UInt32)
INSTANTIATE(UInt64)

#undef INSTANTIATE
}

}
