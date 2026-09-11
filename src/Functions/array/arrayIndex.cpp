#include <Functions/array/arrayIndex.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace DB
{

#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(

template <typename T>
size_t findUIntSIMD(const T * data, size_t size, T value)
{
    static_assert(
        std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32>
        || std::is_same_v<T, UInt64>);

    constexpr size_t lanes = sizeof(__m256i) / sizeof(T);

    /// Check one vector scalarly so a hit at the beginning does not pay SIMD setup costs.
    size_t i = 0;
    for (; i < lanes && i < size; ++i)
        if (data[i] == value)
            return i;

    __m256i needle;
    if constexpr (std::is_same_v<T, UInt8>)
        needle = _mm256_set1_epi8(static_cast<char>(value));
    else if constexpr (std::is_same_v<T, UInt16>)
        needle = _mm256_set1_epi16(static_cast<short>(value));
    else if constexpr (std::is_same_v<T, UInt32>)
        needle = _mm256_set1_epi32(static_cast<int>(value));
    else
        needle = _mm256_set1_epi64x(static_cast<long long>(value));

    for (; i + lanes <= size; i += lanes)
    {
        const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
        __m256i equal;
        if constexpr (std::is_same_v<T, UInt8>)
            equal = _mm256_cmpeq_epi8(values, needle);
        else if constexpr (std::is_same_v<T, UInt16>)
            equal = _mm256_cmpeq_epi16(values, needle);
        else if constexpr (std::is_same_v<T, UInt32>)
            equal = _mm256_cmpeq_epi32(values, needle);
        else
            equal = _mm256_cmpeq_epi64(values, needle);

        unsigned mask;
        if constexpr (std::is_same_v<T, UInt8>)
        {
            mask = static_cast<unsigned>(_mm256_movemask_epi8(equal));
        }
        else if constexpr (std::is_same_v<T, UInt16>)
        {
            /// Each matching 16-bit lane produces two set bytes. Keep the low byte of each lane.
            mask = static_cast<unsigned>(_mm256_movemask_epi8(equal)) & 0x55555555u;
        }
        else if constexpr (std::is_same_v<T, UInt32>)
        {
            mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(equal)));
        }
        else
        {
            mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(equal)));
        }

        if (mask)
        {
            unsigned lane = static_cast<unsigned>(__builtin_ctz(mask));
            if constexpr (std::is_same_v<T, UInt16>)
                lane >>= 1;
            return i + lane;
        }
    }

    for (; i < size; ++i)
        if (data[i] == value)
            return i;

    return static_cast<size_t>(-1);
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE

namespace ArrayIndexImpl
{

size_t findUInt(const UInt8 * data, size_t size, UInt8 value)
{
    return TargetSpecific::x86_64_v3::findUIntSIMD(data, size, value);
}

size_t findUInt(const UInt16 * data, size_t size, UInt16 value)
{
    return TargetSpecific::x86_64_v3::findUIntSIMD(data, size, value);
}

size_t findUInt(const UInt32 * data, size_t size, UInt32 value)
{
    return TargetSpecific::x86_64_v3::findUIntSIMD(data, size, value);
}

size_t findUInt(const UInt64 * data, size_t size, UInt64 value)
{
    return TargetSpecific::x86_64_v3::findUIntSIMD(data, size, value);
}

}
#endif

}
