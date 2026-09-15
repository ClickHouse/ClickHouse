#include <base/defines.h>
#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Functions/array/FunctionArrayMapped.h>

#include <Common/NaNUtils.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <type_traits>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{

enum class ArrayMinMaxIndexStrategy : uint8_t
{
    Min,
    Max,
};

#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(

template <typename T>
size_t findFirstEqualSIMD(const T * data, size_t size, const T & value)
{
    static_assert(
        std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64>
        || std::is_same_v<T, Int8> || std::is_same_v<T, Int16> || std::is_same_v<T, Int32> || std::is_same_v<T, Int64>
        || std::is_same_v<T, Float32> || std::is_same_v<T, Float64>);

    constexpr size_t vector_bytes = sizeof(__m256i);
    constexpr size_t lanes = vector_bytes / sizeof(T);
    constexpr size_t scalar_prefix = 4;
    const size_t prefix_size = std::min(size, scalar_prefix);

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (data[i] == value)
            return i;
    }

    const auto findInVector = [&](size_t offset)
    {
        unsigned mask;

        if constexpr (std::is_same_v<T, Float32>)
        {
            const auto values = _mm256_loadu_ps(data + offset);
            const auto target = _mm256_set1_ps(value);
            mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(values, target, _CMP_EQ_OQ)));
        }
        else if constexpr (std::is_same_v<T, Float64>)
        {
            const auto values = _mm256_loadu_pd(data + offset);
            const auto target = _mm256_set1_pd(value);
            mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_cmp_pd(values, target, _CMP_EQ_OQ)));
        }
        else
        {
            const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + offset));
            __m256i equal;

            if constexpr (sizeof(T) == 1)
                equal = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(static_cast<char>(value)));
            else if constexpr (sizeof(T) == 2)
                equal = _mm256_cmpeq_epi16(values, _mm256_set1_epi16(static_cast<short>(value)));
            else if constexpr (sizeof(T) == 4)
                equal = _mm256_cmpeq_epi32(values, _mm256_set1_epi32(static_cast<int>(value)));
            else
                equal = _mm256_cmpeq_epi64(values, _mm256_set1_epi64x(static_cast<long long>(value)));

            if constexpr (sizeof(T) == 1 || sizeof(T) == 2)
                mask = static_cast<unsigned>(_mm256_movemask_epi8(equal));
            else if constexpr (sizeof(T) == 4)
                mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(equal)));
            else
                mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(equal)));
        }

        if (!mask)
            return lanes;

        size_t lane = static_cast<size_t>(__builtin_ctz(mask));
        if constexpr (sizeof(T) == 2)
            lane >>= 1;
        return lane;
    };

    size_t i = prefix_size;
    for (; i + 4 * lanes <= size; i += 4 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;

        const size_t third = findInVector(i + 2 * lanes);
        if (third != lanes)
            return i + 2 * lanes + third;

        const size_t fourth = findInVector(i + 3 * lanes);
        if (fourth != lanes)
            return i + 3 * lanes + fourth;
    }

    for (; i + 2 * lanes <= size; i += 2 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;
    }

    for (; i + lanes <= size; i += lanes)
    {
        const size_t found = findInVector(i);
        if (found != lanes)
            return i + found;
    }

    for (; i < size; ++i)
    {
        if (data[i] == value)
            return i;
    }

    return size;
}

template <typename T>
requires(std::is_same_v<T, UInt64> || std::is_same_v<T, Int64>)
size_t findFirstEqualPackedMaskSIMD(const T * data, size_t size, const T & value)
{
    UInt64 matches = 0;
    const auto target = _mm256_set1_epi64x(static_cast<long long>(value));

    size_t i = 0;
    for (; i + 4 <= size; i += 4)
    {
        const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
        const auto equal = _mm256_cmpeq_epi64(values, target);
        matches |= static_cast<UInt64>(_mm256_movemask_pd(_mm256_castsi256_pd(equal))) << i;
    }

    for (; i < size; ++i)
    {
        if (data[i] == value)
            matches |= UInt64{1} << i;
    }

    return matches ? static_cast<size_t>(__builtin_ctzll(matches)) : size;
}

template <typename T>
requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
size_t findFirstNaNSIMD(const T * data, size_t size)
{
    constexpr size_t lanes = sizeof(__m256i) / sizeof(T);
    constexpr size_t scalar_prefix = 4;
    const size_t prefix_size = std::min(size, scalar_prefix);

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }

    const auto findInVector = [&](size_t offset)
    {
        unsigned mask;
        if constexpr (std::is_same_v<T, Float32>)
        {
            const auto values = _mm256_loadu_ps(data + offset);
            mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(values, values, _CMP_UNORD_Q)));
        }
        else
        {
            const auto values = _mm256_loadu_pd(data + offset);
            mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_cmp_pd(values, values, _CMP_UNORD_Q)));
        }

        if (!mask)
            return lanes;
        return static_cast<size_t>(__builtin_ctz(mask));
    };

    size_t i = prefix_size;
    for (; i + 4 * lanes <= size; i += 4 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;

        const size_t third = findInVector(i + 2 * lanes);
        if (third != lanes)
            return i + 2 * lanes + third;

        const size_t fourth = findInVector(i + 3 * lanes);
        if (fourth != lanes)
            return i + 3 * lanes + fourth;
    }

    for (; i + 2 * lanes <= size; i += 2 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;
    }

    for (; i + lanes <= size; i += lanes)
    {
        const size_t found = findInVector(i);
        if (found != lanes)
            return i + found;
    }

    for (; i < size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }

    return size;
}

)

DECLARE_X86_64_V4_SPECIFIC_CODE(

template <typename T>
requires(
    std::is_same_v<T, UInt32> || std::is_same_v<T, Int32> || std::is_same_v<T, UInt64>
    || std::is_same_v<T, Int64>)
__m512i addIndexVector(__m512i indices, size_t offset)
{
    if constexpr (sizeof(T) == 4)
    {
        return _mm512_add_epi32(indices, _mm512_set1_epi32(static_cast<int>(offset)));
    }
    else
    {
        return _mm512_add_epi64(indices, _mm512_set1_epi64(static_cast<long long>(offset)));
    }
}

template <typename T>
requires(
    std::is_same_v<T, UInt32> || std::is_same_v<T, Int32> || std::is_same_v<T, UInt64>
    || std::is_same_v<T, Int64>)
__m512i makeIndexVector(size_t offset)
{
    if constexpr (sizeof(T) == 4)
        return addIndexVector<T>(_mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), offset);
    else
        return addIndexVector<T>(_mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7), offset);
}

template <typename T>
requires(std::is_same_v<T, UInt64> || std::is_same_v<T, Int64>)
size_t findFirstEqualPackedMaskSIMD(const T * data, size_t size, const T & value)
{
    UInt64 matches = 0;
    const auto target = _mm512_set1_epi64(static_cast<long long>(value));

    size_t i = 0;
    for (; i + 8 <= size; i += 8)
    {
        const auto values = _mm512_loadu_si512(data + i);
        const auto equal = _mm512_cmpeq_epi64_mask(values, target);
        matches |= static_cast<UInt64>(equal) << i;
    }

    for (; i < size; ++i)
    {
        if (data[i] == value)
            matches |= UInt64{1} << i;
    }

    return matches ? static_cast<size_t>(__builtin_ctzll(matches)) : size;
}

template <ArrayMinMaxIndexStrategy strategy, typename T, size_t accumulator_count>
requires(
    std::is_same_v<T, UInt32> || std::is_same_v<T, Int32> || std::is_same_v<T, UInt64>
    || std::is_same_v<T, Int64>)
size_t findIndexAVX512(const T * data, size_t size)
{
    constexpr size_t lanes = sizeof(__m512i) / sizeof(T);
    constexpr size_t stride = accumulator_count * lanes;
    static_assert(accumulator_count == 1 || accumulator_count == 2 || accumulator_count == 4);

    const auto is_better = [](T lhs, T rhs)
    {
        if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
            return lhs < rhs;
        else
            return lhs > rhs;
    };

    const auto update = [&](const __m512i values, const __m512i indices, __m512i & best_values, __m512i & best_indices)
    {
        if constexpr (std::is_same_v<T, UInt32>)
        {
            constexpr int comparison = strategy == ArrayMinMaxIndexStrategy::Min ? _MM_CMPINT_LT : _MM_CMPINT_GT;
            const __mmask16 better = _mm512_cmp_epu32_mask(values, best_values, comparison);
            best_values = _mm512_mask_blend_epi32(better, best_values, values);
            best_indices = _mm512_mask_blend_epi32(better, best_indices, indices);
        }
        else if constexpr (std::is_same_v<T, Int32>)
        {
            constexpr int comparison = strategy == ArrayMinMaxIndexStrategy::Min ? _MM_CMPINT_LT : _MM_CMPINT_GT;
            const __mmask16 better = _mm512_cmp_epi32_mask(values, best_values, comparison);
            best_values = _mm512_mask_blend_epi32(better, best_values, values);
            best_indices = _mm512_mask_blend_epi32(better, best_indices, indices);
        }
        else if constexpr (std::is_same_v<T, UInt64>)
        {
            constexpr int comparison = strategy == ArrayMinMaxIndexStrategy::Min ? _MM_CMPINT_LT : _MM_CMPINT_GT;
            const __mmask8 better = _mm512_cmp_epu64_mask(values, best_values, comparison);
            best_values = _mm512_mask_blend_epi64(better, best_values, values);
            best_indices = _mm512_mask_blend_epi64(better, best_indices, indices);
        }
        else
        {
            constexpr int comparison = strategy == ArrayMinMaxIndexStrategy::Min ? _MM_CMPINT_LT : _MM_CMPINT_GT;
            const __mmask8 better = _mm512_cmp_epi64_mask(values, best_values, comparison);
            best_values = _mm512_mask_blend_epi64(better, best_values, values);
            best_indices = _mm512_mask_blend_epi64(better, best_indices, indices);
        }
    };

    std::array<__m512i, accumulator_count> best_values;
    std::array<__m512i, accumulator_count> best_indices;
    std::array<__m512i, accumulator_count> current_indices;
    for (size_t accumulator = 0; accumulator < accumulator_count; ++accumulator)
    {
        const size_t offset = accumulator * lanes;
        best_values[accumulator] = _mm512_loadu_si512(data + offset);
        best_indices[accumulator] = makeIndexVector<T>(offset);
        current_indices[accumulator] = best_indices[accumulator];
    }

    size_t i = stride;
    for (; i + stride <= size; i += stride)
    {
        for (size_t accumulator = 0; accumulator < accumulator_count; ++accumulator)
        {
            current_indices[accumulator] = addIndexVector<T>(current_indices[accumulator], stride);
            update(
                _mm512_loadu_si512(data + i + accumulator * lanes),
                current_indices[accumulator],
                best_values[accumulator],
                best_indices[accumulator]);
        }
    }

    const size_t remaining_vectors = (size - i) / lanes;
    for (size_t accumulator = 0; accumulator < remaining_vectors; ++accumulator)
    {
        current_indices[accumulator] = addIndexVector<T>(current_indices[accumulator], stride);
        update(
            _mm512_loadu_si512(data + i + accumulator * lanes),
            current_indices[accumulator],
            best_values[accumulator],
            best_indices[accumulator]);
    }
    i += remaining_vectors * lanes;

    const auto reduce_value = [](const __m512i values) -> T
    {
        if constexpr (std::is_same_v<T, UInt32>)
        {
            if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
                return _mm512_reduce_min_epu32(values);
            else
                return _mm512_reduce_max_epu32(values);
        }
        else if constexpr (std::is_same_v<T, Int32>)
        {
            if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
                return _mm512_reduce_min_epi32(values);
            else
                return _mm512_reduce_max_epi32(values);
        }
        else if constexpr (std::is_same_v<T, UInt64>)
        {
            if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
                return _mm512_reduce_min_epu64(values);
            else
                return _mm512_reduce_max_epu64(values);
        }
        else
        {
            if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
                return _mm512_reduce_min_epi64(values);
            else
                return _mm512_reduce_max_epi64(values);
        }
    };

    T best = reduce_value(best_values[0]);
    for (size_t accumulator = 1; accumulator < accumulator_count; ++accumulator)
    {
        const T value = reduce_value(best_values[accumulator]);
        if (is_better(value, best))
            best = value;
    }

    size_t best_index = size;
    if constexpr (sizeof(T) == 4)
    {
        const __m512i target = _mm512_set1_epi32(static_cast<int>(best));
        for (size_t accumulator = 0; accumulator < accumulator_count; ++accumulator)
        {
            const __mmask16 matches = _mm512_cmpeq_epi32_mask(best_values[accumulator], target);
            if (matches)
            {
                const size_t index = static_cast<size_t>(_mm512_mask_reduce_min_epu32(matches, best_indices[accumulator]));
                best_index = std::min(best_index, index);
            }
        }
    }
    else
    {
        const __m512i target = _mm512_set1_epi64(static_cast<long long>(best));
        for (size_t accumulator = 0; accumulator < accumulator_count; ++accumulator)
        {
            const __mmask8 matches = _mm512_cmpeq_epi64_mask(best_values[accumulator], target);
            if (matches)
            {
                const size_t index = static_cast<size_t>(_mm512_mask_reduce_min_epu64(matches, best_indices[accumulator]));
                best_index = std::min(best_index, index);
            }
        }
    }

    const auto consider = [&](T value, size_t index)
    {
        if (is_better(value, best) || (value == best && index < best_index))
        {
            best = value;
            best_index = index;
        }
    };

    for (; i < size; ++i)
        consider(data[i], i);

    return best_index;
}
)
#endif

namespace ArrayMinMaxIndexImpl
{

constexpr size_t small_tournament_limit = 32;
constexpr size_t small_tournament_limit_64_bit = 48;
constexpr size_t medium_two_pass_limit = 256;
constexpr size_t two_pass_limit_64_bit_integer = 16384;
constexpr size_t avx512_two_accumulator_min_size = 128;
constexpr size_t avx512_four_accumulator_min_size = 512;
constexpr size_t packed_mask_min_size = 8;
constexpr size_t avx2_packed_mask_max_size = 32;
constexpr size_t avx512_packed_mask_max_size = 64;

/*
 * These are measured performance cutovers for the AVX2 path. The small
 * tournament wins up to 32 elements (48 for 64-bit integers), the packed-mask
 * path covers short 64-bit rows, the two-pass SIMD path is capped at 256 for
 * other types, and 64-bit arrays stay on the two-pass path until record blocks
 * amortize their extra lookup work at 16384. x86-64-v4 uses a one-pass indexed
 * reduction after the packed-mask path, increasing from one to two and then
 * four accumulators at 128 and 512 elements.
 */

template <typename T>
constexpr size_t smallTournamentLimit()
{
    if constexpr (std::is_integral_v<T> && sizeof(T) == 8)
        return small_tournament_limit_64_bit;
    else if constexpr (std::is_integral_v<T>)
        return small_tournament_limit;
    else
        return 0;
}

template <typename T>
constexpr size_t mediumTwoPassMinSize()
{
    if constexpr (std::is_integral_v<T>)
        return smallTournamentLimit<T>();
    else
        return 64;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T> || std::is_floating_point_v<T>)
constexpr T terminalValue()
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
            return -std::numeric_limits<T>::infinity();
        else
            return std::numeric_limits<T>::infinity();
    }
    else
    {
        if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
            return std::numeric_limits<T>::lowest();
        else
            return std::numeric_limits<T>::max();
    }
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static bool isBetter(const T & lhs, const T & rhs)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return lhs < rhs;
    else
        return lhs > rhs;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T>)
static std::optional<T> findExtremeValue(const T * data, size_t begin, size_t end)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return findExtremeMin(data, begin, end);
    else
        return findExtremeMax(data, begin, end);
}

static bool useAVX2()
{
#if USE_MULTITARGET_CODE
    return isArchSupported(TargetArch::x86_64_v3);
#else
    return false;
#endif
}

static bool useAVX512()
{
#if USE_MULTITARGET_CODE
    return isArchSupported(TargetArch::x86_64_v4);
#else
    return false;
#endif
}

template <typename T>
static size_t findFirstEqual(const T * data, size_t size, const T & value, bool use_simd)
{
#if USE_MULTITARGET_CODE
    if (use_simd)
        return TargetSpecific::x86_64_v3::findFirstEqualSIMD(data, size, value);
#else
    (void)use_simd;
#endif

    for (size_t i = 0; i < size; ++i)
    {
        if (data[i] == value)
            return i;
    }
    return size;
}

template <typename T>
requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
static size_t findFirstNaN(const T * data, size_t size, bool use_simd)
{
#if USE_MULTITARGET_CODE
    if (use_simd)
        return TargetSpecific::x86_64_v3::findFirstNaNSIMD(data, size);
#else
    (void)use_simd;
#endif

    for (size_t i = 0; i < size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }
    return size;
}

template <typename T>
static size_t findFirstSelectedValue(const T * data, size_t size, const T & value, bool use_simd)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (isNaN(value))
            return findFirstNaN(data, size, use_simd);
    }
    return findFirstEqual(data, size, value, use_simd);
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_same_v<T, UInt64> || std::is_same_v<T, Int64>)
static size_t findIndexPackedMask(const T * data, size_t size, bool use_simd, bool use_avx512)
{
    const auto extreme = findExtremeValue<strategy>(data, 0, size);
    chassert(extreme.has_value());

#if USE_MULTITARGET_CODE
    if (use_avx512)
        return TargetSpecific::x86_64_v4::findFirstEqualPackedMaskSIMD(data, size, *extreme);
    if (use_simd)
        return TargetSpecific::x86_64_v3::findFirstEqualPackedMaskSIMD(data, size, *extreme);
#else
    (void)use_simd;
    (void)use_avx512;
#endif

    return findFirstEqual(data, size, *extreme, false);
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T>)
static size_t findIndexTwoPass(const T * data, size_t size, bool use_simd)
{
    const auto extreme = findExtremeValue<strategy>(data, 0, size);
    chassert(extreme.has_value());

    const size_t index = findFirstSelectedValue(data, size, *extreme, use_simd);
    chassert(index < size);
    return index;
}

template <typename T>
requires(std::is_integral_v<T>)
struct IndexedValue
{
    T value;
    size_t index;
};

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static IndexedValue<T> selectIndexedValue(const IndexedValue<T> & lhs, const IndexedValue<T> & rhs)
{
    return isBetter<strategy>(rhs.value, lhs.value) ? rhs : lhs;
}

template <size_t count, ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static IndexedValue<T> selectIndexedBlock(const T * data, size_t offset)
{
    if constexpr (count == 1)
        return {data[offset], offset};
    else
        return selectIndexedValue<strategy>(
            selectIndexedBlock<count / 2, strategy>(data, offset),
            selectIndexedBlock<count / 2, strategy>(data, offset + count / 2));
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static size_t findIndexTournament(const T * data, size_t size)
{
    bool have_best = false;
    IndexedValue<T> best{};

    const auto add = [&](const IndexedValue<T> & value)
    {
        if (!have_best)
        {
            best = value;
            have_best = true;
        }
        else
        {
            best = selectIndexedValue<strategy>(best, value);
        }
    };

    size_t i = 0;
    for (; i + 16 <= size; i += 16)
        add(selectIndexedBlock<16, strategy>(data, i));
    if (i + 8 <= size)
    {
        add(selectIndexedBlock<8, strategy>(data, i));
        i += 8;
    }
    if (i + 4 <= size)
    {
        add(selectIndexedBlock<4, strategy>(data, i));
        i += 4;
    }
    if (i + 2 <= size)
    {
        add(selectIndexedBlock<2, strategy>(data, i));
        i += 2;
    }
    if (i < size)
        add({data[i], i});

    return best.index;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndexOnePass(const T * data, size_t size)
{
    size_t index = 0;

    if constexpr (std::is_floating_point_v<T>)
    {
        while (index < size && isNaN(data[index]))
            ++index;

        if (index == size)
            return 0;
    }

    T best = data[index];

#pragma clang loop unroll_count(8)
    for (size_t i = index + 1; i < size; ++i)
    {
        if (isBetter<strategy>(data[i], best))
        {
            best = data[i];
            index = i;
        }
    }

    return index;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndexSmallOrOnePass(const T * data, size_t size)
{
    if constexpr (std::is_integral_v<T>)
    {
        if (size <= smallTournamentLimit<T>())
            return findIndexTournament<strategy>(data, size);
    }
    return findIndexOnePass<strategy>(data, size);
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T>)
static size_t findIndexRecordBlocks(const T * data, size_t size, bool use_simd)
{
    constexpr size_t block_size = 1024;

    T best{};
    size_t best_block_begin = 0;
    size_t best_block_end = 0;
    [[maybe_unused]] size_t first_nan = size;
    bool have_numeric_value = !std::is_floating_point_v<T>;

    if constexpr (std::is_floating_point_v<T>)
    {
        if (isNaN(data[0]))
            first_nan = 0;
        else
        {
            best = data[0];
            best_block_end = std::min(block_size, size);
            have_numeric_value = true;
        }
    }
    else
    {
        /// Treat the first block as a record so a terminal integer can stop the scan immediately.
        have_numeric_value = false;
    }

    for (size_t block_begin = 0; block_begin < size; block_begin += block_size)
    {
        const size_t block_end = std::min(block_begin + block_size, size);
        const auto block_extreme = findExtremeValue<strategy>(data, block_begin, block_end);

        chassert(block_extreme.has_value());

        if constexpr (std::is_floating_point_v<T>)
        {
            if (isNaN(*block_extreme))
            {
                if (first_nan == size)
                {
                    const size_t nan_index = findFirstNaN(data + block_begin, block_end - block_begin, use_simd);
                    chassert(nan_index < block_end - block_begin);
                    first_nan = block_begin + nan_index;
                }
                continue;
            }
        }

        const bool record = !have_numeric_value || isBetter<strategy>(*block_extreme, best);

        if (record)
        {
            best = *block_extreme;
            best_block_begin = block_begin;
            best_block_end = block_end;
            have_numeric_value = true;

            if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>)
            {
                if (block_end < size && unlikely(best == terminalValue<strategy, T>()))
                {
                    const size_t block_index = findFirstSelectedValue(
                        data + best_block_begin, best_block_end - best_block_begin, best, use_simd);
                    chassert(block_index < best_block_end - best_block_begin);
                    return best_block_begin + block_index;
                }
            }
        }
    }

    if constexpr (std::is_floating_point_v<T>)
    {
        if (!have_numeric_value)
        {
            chassert(first_nan < size);
            return first_nan;
        }
    }

    const size_t block_index = findFirstSelectedValue(
        data + best_block_begin, best_block_end - best_block_begin, best, use_simd);
    chassert(block_index < best_block_end - best_block_begin);
    return best_block_begin + block_index;
}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
requires(is_big_int_v<Element>)
static void executeWideNumericData(const Element * data, const ColumnArray::Offsets & offsets, ColumnUInt32::Container & result)
{
    size_t begin = 0;
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        const size_t end = offsets[row];
        const size_t size = end - begin;
        result[row] = size <= 1
            ? static_cast<UInt32>(size)
            : static_cast<UInt32>(findIndexOnePass<strategy>(data + begin, size) + 1);
        begin = end;
    }
}

/// Wide integers do not have a SIMD findExtreme implementation. Keep them on a typed scan
/// instead of paying the generic compareAt dispatch for every element.
template <ArrayMinMaxIndexStrategy strategy, typename Element>
requires(is_big_int_v<Element>)
static bool executeWideNumeric(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);
    if (!column)
        return false;

    auto result_column = ColumnUInt32::create(offsets.size());
    executeWideNumericData<strategy, Element>(column->getData().data(), offsets, result_column->getData());

    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
static void executeNumericData(const Element * data, const ColumnArray::Offsets & offsets, ColumnUInt32::Container & result)
{
    const bool use_simd = useAVX2();
    const bool use_avx512 = useAVX512();

    size_t begin = 0;
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        const size_t end = offsets[row];
        const size_t size = end - begin;

        if constexpr (std::is_integral_v<Element> || std::is_floating_point_v<Element>)
        {
            if (size > 1 && unlikely(data[begin] == terminalValue<strategy, Element>()))
            {
                result[row] = 1;
                begin = end;
                continue;
            }
        }

        constexpr size_t medium_two_pass_min_size = mediumTwoPassMinSize<Element>();

#if USE_MULTITARGET_CODE
        if constexpr (
            std::is_same_v<Element, UInt32> || std::is_same_v<Element, Int32>
            || std::is_same_v<Element, UInt64> || std::is_same_v<Element, Int64>)
        {
            if constexpr (std::is_same_v<Element, UInt64> || std::is_same_v<Element, Int64>)
            {
                if (use_avx512 && size >= packed_mask_min_size && size <= avx512_packed_mask_max_size)
                {
                    result[row] = static_cast<UInt32>(
                        findIndexPackedMask<strategy, Element>(data + begin, size, false, true) + 1);
                    begin = end;
                    continue;
                }

                if (use_simd && size >= packed_mask_min_size && size <= avx2_packed_mask_max_size)
                {
                    result[row] = static_cast<UInt32>(
                        findIndexPackedMask<strategy, Element>(data + begin, size, true, false) + 1);
                    begin = end;
                    continue;
                }
            }

            if (use_avx512 && size > smallTournamentLimit<Element>())
            {
                if (size >= avx512_four_accumulator_min_size)
                {
                    result[row] = static_cast<UInt32>(
                        TargetSpecific::x86_64_v4::findIndexAVX512<strategy, Element, 4>(data + begin, size) + 1);
                }
                else if (size >= avx512_two_accumulator_min_size)
                {
                    result[row] = static_cast<UInt32>(
                        TargetSpecific::x86_64_v4::findIndexAVX512<strategy, Element, 2>(data + begin, size) + 1);
                }
                else
                {
                    result[row] = static_cast<UInt32>(
                        TargetSpecific::x86_64_v4::findIndexAVX512<strategy, Element, 1>(data + begin, size) + 1);
                }
                begin = end;
                continue;
            }
        }

#endif

        if (size <= 1)
        {
            result[row] = static_cast<UInt32>(size);
        }
        else if (size <= medium_two_pass_limit && size > medium_two_pass_min_size && use_simd)
        {
            std::array<Element, 4> extrema{};
            std::array<const Element *, 4> row_data{};
            std::array<size_t, 4> row_sizes{};
            size_t tile_rows = 0;
            size_t tile_begin = begin;

            while (tile_rows < extrema.size() && row + tile_rows < offsets.size())
            {
                const size_t tile_end = offsets[row + tile_rows];
                const size_t tile_size = tile_end - tile_begin;
                if (tile_size <= medium_two_pass_min_size || tile_size > medium_two_pass_limit)
                    break;

                row_data[tile_rows] = data + tile_begin;
                row_sizes[tile_rows] = tile_size;
                extrema[tile_rows] = *findExtremeValue<strategy>(data, tile_begin, tile_end);
                tile_begin = tile_end;
                ++tile_rows;
            }

            chassert(tile_rows != 0);
            for (size_t tile_row = 0; tile_row < tile_rows; ++tile_row)
            {
                const size_t index = findFirstSelectedValue(
                    row_data[tile_row], row_sizes[tile_row], extrema[tile_row], true);
                chassert(index < row_sizes[tile_row]);
                result[row + tile_row] = static_cast<UInt32>(index + 1);
            }
            row += tile_rows - 1;
            begin = tile_begin;
            continue;
        }
        else if (size <= 64 || !use_simd)
        {
            result[row] = static_cast<UInt32>(findIndexSmallOrOnePass<strategy>(data + begin, size) + 1);
        }
        else if (std::is_integral_v<Element> && sizeof(Element) == 8 && size < two_pass_limit_64_bit_integer)
        {
            result[row] = static_cast<UInt32>(findIndexTwoPass<strategy>(data + begin, size, true) + 1);
        }
        else
        {
            result[row] = static_cast<UInt32>(findIndexRecordBlocks<strategy>(data + begin, size, true) + 1);
        }

        begin = end;
    }

}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
static bool executeNumeric(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);
    if (!column)
        return false;

    auto result_column = ColumnUInt32::create(offsets.size());
    executeNumericData<strategy, Element>(column->getData().data(), offsets, result_column->getData());
    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy, typename Decimal>
requires(is_decimal<Decimal>)
static bool executeDecimal(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnDecimal<Decimal>>(&*mapped);
    if (!column)
        return false;

    using Element = typename Decimal::NativeType;
    static_assert(sizeof(Decimal) == sizeof(Element));
    static_assert(alignof(Decimal) == alignof(Element));
    const auto * data = reinterpret_cast<const Element *>(column->getData().data());
    auto result_column = ColumnUInt32::create(offsets.size());
    if constexpr (is_big_int_v<Element>)
        executeWideNumericData<strategy, Element>(data, offsets, result_column->getData());
    else
        executeNumericData<strategy, Element>(data, offsets, result_column->getData());
    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy>
struct ArrayMinMaxIndexImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr &, const DataTypePtr &)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto & offsets = array.getOffsets();

        if (checkAndGetColumn<ColumnConst>(&*mapped))
        {
            auto result = ColumnUInt32::create(offsets.size());
            auto & result_data = result->getData();
            size_t begin = 0;
            for (size_t row = 0; row < offsets.size(); ++row)
            {
                const size_t end = offsets[row];
                result_data[row] = begin == end ? 0 : 1;
                begin = end;
            }
            return result;
        }

        ColumnPtr numeric_result;
        if (executeNumeric<strategy, UInt8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt64>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int64>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, UInt128>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, UInt256>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, Int128>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, Int256>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float64>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal32>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal64>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal128>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal256>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, DateTime64>(mapped, offsets, numeric_result))
            return numeric_result;

        static constexpr int nan_null_direction_hint = strategy == ArrayMinMaxIndexStrategy::Min ? 1 : -1;

        auto result = ColumnUInt32::create(offsets.size());
        auto & result_data = result->getData();
        size_t begin = 0;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const size_t end = offsets[row];
            if (begin == end)
            {
                result_data[row] = 0;
                begin = end;
                continue;
            }

            size_t best = begin;
            for (size_t i = begin + 1; i < end; ++i)
            {
                const int comparison = mapped->compareAt(i, best, *mapped, nan_null_direction_hint);
                if (isBetter<strategy>(comparison, 0))
                    best = i;
            }

            result_data[row] = static_cast<UInt32>(best - begin + 1);
            begin = end;
        }

        return result;
    }
};

struct NameArrayMinIndex { static constexpr auto name = "arrayMinIndex"; };
using FunctionArrayMinIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Min>, NameArrayMinIndex>;

struct NameArrayMaxIndex { static constexpr auto name = "arrayMaxIndex"; };
using FunctionArrayMaxIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Max>, NameArrayMaxIndex>;

REGISTER_FUNCTION(ArrayMinMaxIndex)
{
    FunctionDocumentation::Description description_min = R"(
Returns the 1-based index of the minimum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the minimum lambda result. If multiple elements have the same minimum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_min = "arrayMinIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_min = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_min = {"Returns the 1-based index of the first minimum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_min = {
        {"Basic example", "SELECT arrayMinIndex([5, 3, 2, 7]);", "3"},
        {"First equal minimum", "SELECT arrayMinIndex([5, 3, 3, 7]);", "2"},
        {"Usage with lambda function", "SELECT arrayMinIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_min = {26, 9};
    FunctionDocumentation::Category category_min = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_min = {description_min, syntax_min, arguments_min, {}, returned_value_min, examples_min, introduced_in_min, category_min};
    factory.registerFunction<FunctionArrayMinIndex>(documentation_min);

    FunctionDocumentation::Description description_max = R"(
Returns the 1-based index of the maximum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the maximum lambda result. If multiple elements have the same maximum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_max = "arrayMaxIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_max = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_max = {"Returns the 1-based index of the first maximum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_max = {
        {"Basic example", "SELECT arrayMaxIndex([5, 3, 2, 7]);", "4"},
        {"First equal maximum", "SELECT arrayMaxIndex([5, 7, 7, 3]);", "2"},
        {"Usage with lambda function", "SELECT arrayMaxIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "3"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_max = {26, 9};
    FunctionDocumentation::Category category_max = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_max = {description_max, syntax_max, arguments_max, {}, returned_value_max, examples_max, introduced_in_max, category_max};
    factory.registerFunction<FunctionArrayMaxIndex>(documentation_max);
}

}
