#pragma once

#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/sliceEqualElements.h>

#include <Common/TargetSpecific.h>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if USE_ARM_MULTITARGET_CODE
#include <arm_sve.h>
#endif

namespace DB::GatherUtils
{

inline ALWAYS_INLINE bool hasNull(const UInt8 * null_map, size_t null_map_size)
{
    if (null_map == nullptr)
        return false;

    for (size_t i = 0; i < null_map_size; ++i)
    {
        if (null_map[i])
            return true;
    }

    return false;
}

template<class T>
inline ALWAYS_INLINE bool hasAllIntegralLoopRemainder(
    size_t j, const NumericArraySlice<T> & first, const NumericArraySlice<T> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    for (; j < second.size; ++j)
    {
        // skip null elements since both have at least one - assuming it was checked earlier that at least one element in 'first' is null
        if (has_second_null_map && second_null_map[j])
            continue;

        bool found = false;

        for (size_t i = 0; i < first.size; ++i)
        {
            if (has_first_null_map && first_null_map[i])
                continue;

            if (first.data[i] == second.data[j])
            {
                found = true;
                break;
            }
        }

        if (!found)
            return false;
    }
    return true;
}

#if defined(__AVX2__)

// AVX2 Int64, UInt64 specialization
template<typename IntType>
requires (std::is_same_v<IntType, Int64> || std::is_same_v<IntType, UInt64>)
NO_INLINE bool sliceHasImplAnyAllImplInt64(
    const NumericArraySlice<IntType> & first,
    const NumericArraySlice<IntType> & second,
    const UInt8 * first_null_map,
    const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    int has_mask = 1;
    static constexpr Int64 full = -1;
    static constexpr Int64 none = 0;
    const __m256i ones = _mm256_set1_epi64x(full);
    const __m256i zeros = _mm256_setzero_si256();

    if (second.size > 3 && first.size > 3)
    {
        for (; j < second.size - 3 && has_mask; j += 4)
        {
            has_mask = 0;
            const __m256i second_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            // bits of the bitmask are set to one if considered as null in the corresponding null map, 0 otherwise;
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi64x(
                    (second_null_map[j + 3])? full : none,
                    (second_null_map[j + 2])? full : none,
                    (second_null_map[j + 1])? full : none,
                    (second_null_map[j]) ? full : none)
                : zeros;

            size_t i = 0;
            for (; i < first.size - 3 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 4)
            {
                const __m256i first_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first.data + i));
                const __m256i first_nm_mask = has_first_null_map?
                    _mm256_set_m128i(
                        _mm_cvtepi8_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i + 2))),
                        _mm_cvtepi8_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))))
                    : zeros;
                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        first_nm_mask,
                                        _mm256_cmpeq_epi64(second_data, first_data)),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(5,4,3,2,1,0,7,6)),
                                        _mm256_cmpeq_epi64(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(5,4,3,2,1,0,7,6))))),

                            _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(3,2,1,0,7,6,5,4)),
                                        _mm256_cmpeq_epi64(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(3,2,1,0,7,6,5,4)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(1,0,7,6,5,4,3,2)),
                                        _mm256_cmpeq_epi64(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(1,0,7,6,5,4,3,2)))))),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;

                    __m256i v_i = _mm256_set1_epi64x(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi64(second_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask && second.size > 3)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// AVX2 Int32, UInt32 specialization
template<typename IntType>
requires (std::is_same_v<IntType, Int32> || std::is_same_v<IntType, UInt32>)
NO_INLINE bool sliceHasImplAnyAllImplInt32(
    const NumericArraySlice<IntType> & first,
    const NumericArraySlice<IntType> & second,
    const UInt8 * first_null_map,
    const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    size_t j = 0;
    int has_mask = 1;
    static constexpr int full = -1;
    static constexpr int none = 0;

    const __m256i ones = _mm256_set1_epi32(full);
    const __m256i zeros = _mm256_setzero_si256();

    if (second.size > 7 && first.size > 7)
    {
        for (; j < second.size - 7 && has_mask; j += 8)
        {
            has_mask = 0;
            const __m256i second_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            // bits of the bitmask are set to one if considered as null in the corresponding null map, 0 otherwise;
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi32(
                    (second_null_map[j + 7]) ? full : none,
                    (second_null_map[j + 6]) ? full : none,
                    (second_null_map[j + 5]) ? full : none,
                    (second_null_map[j + 4]) ? full : none,
                    (second_null_map[j + 3]) ? full : none,
                    (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none,
                    (second_null_map[j]) ? full : none)
                : zeros;

            size_t i = 0;
            for (; i < first.size - 7 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 8)
            {
                const __m256i first_data = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(first.data + i));
                // Create a mask to avoid to compare null elements
                // set_m128i takes two arguments: (high segment, low segment) that are two __m128i convert from 8bits to 32bits to match with next operations
                const __m256i first_nm_mask = has_first_null_map?
                    _mm256_set_m128i(
                        _mm_cvtepi8_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i + 4))),
                        _mm_cvtepi8_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))))
                    : zeros;
                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        first_nm_mask,
                                        _mm256_cmpeq_epi32(second_data, first_data)),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(6,5,4,3,2,1,0,7)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(6,5,4,3,2,1,0,7))))),
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(5,4,3,2,1,0,7,6)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(5,4,3,2,1,0,7,6)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(4,3,2,1,0,7,6,5)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(4,3,2,1,0,7,6,5)))))
                            ),
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(3,2,1,0,7,6,5,4)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(3,2,1,0,7,6,5,4)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(2,1,0,7,6,5,4,3)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(2,1,0,7,6,5,4,3))))),
                                _mm256_or_si256(
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(1,0,7,6,5,4,3,2)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(1,0,7,6,5,4,3,2)))),
                                    _mm256_andnot_si256(
                                        _mm256_permutevar8x32_epi32(first_nm_mask, _mm256_set_epi32(0,7,6,5,4,3,2,1)),
                                        _mm256_cmpeq_epi32(second_data, _mm256_permutevar8x32_epi32(first_data, _mm256_set_epi32(0,7,6,5,4,3,2,1))))))),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;

                    __m256i v_i = _mm256_set1_epi32(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi32(second_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask && second.size > 7)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// AVX2 Int16, UInt16 specialization
template<typename IntType>
requires (std::is_same_v<IntType, Int16> || std::is_same_v<IntType, UInt16>)
NO_INLINE bool sliceHasImplAnyAllImplInt16(
    const NumericArraySlice<IntType> & first,
    const NumericArraySlice<IntType> & second,
    const UInt8 * first_null_map,
    const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map  != nullptr;

    size_t j = 0;
    int has_mask = 1;
    static constexpr int16_t full = -1;
    static constexpr int16_t none = 0;
    const __m256i ones = _mm256_set1_epi16(full);
    const __m256i zeros = _mm256_setzero_si256();
    if (second.size > 15 && first.size > 15)
    {
        for (; j < second.size - 15 && has_mask; j += 16)
        {
            has_mask = 0;
            const __m256i second_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(second.data + j));
            __m256i bitmask = has_second_null_map ?
                _mm256_set_epi16(
                    (second_null_map[j + 15]) ? full : none, (second_null_map[j + 14]) ? full : none,
                    (second_null_map[j + 13]) ? full : none, (second_null_map[j + 12]) ? full : none,
                    (second_null_map[j + 11]) ? full : none, (second_null_map[j + 10]) ? full : none,
                    (second_null_map[j + 9]) ? full : none, (second_null_map[j + 8])? full : none,
                    (second_null_map[j + 7]) ? full : none, (second_null_map[j + 6])? full : none,
                    (second_null_map[j + 5]) ? full : none, (second_null_map[j + 4])? full : none,
                    (second_null_map[j + 3]) ? full : none, (second_null_map[j + 2])? full : none,
                    (second_null_map[j + 1]) ? full : none, (second_null_map[j]) ? full : none)
                : zeros;

            size_t i = 0;
            for (; i < first.size - 15 && !has_mask; has_mask = _mm256_testc_si256(bitmask, ones), i += 16)
            {
                const __m256i first_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first.data + i));
                const __m256i first_nm_mask = has_first_null_map?
                    _mm256_set_m128i(
                        _mm_cvtepi8_epi16(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i + 8))),
                        _mm_cvtepi8_epi16(_mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))))
                    : zeros;

                bitmask =
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            first_nm_mask,
                                            _mm256_cmpeq_epi16(second_data, first_data)),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26)))))
                                ),
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(first_nm_mask, _mm256_set_epi8(17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(first_data, _mm256_set_epi8(17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18))))))
                            ),
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_permute2x128_si256(first_nm_mask, first_nm_mask,1),
                                            _mm256_cmpeq_epi16(second_data, _mm256_permute2x128_si256(first_data, first_data, 1))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(9,8,7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10)))))
                                ),
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(7,6,5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(5,4,3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6))))),
                                    _mm256_or_si256(
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(3,2,1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4)))),
                                        _mm256_andnot_si256(
                                            _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_nm_mask, first_nm_mask, 1), _mm256_set_epi8(1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2)),
                                            _mm256_cmpeq_epi16(second_data, _mm256_shuffle_epi8(_mm256_permute2x128_si256(first_data, first_data, 1), _mm256_set_epi8(1,0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2))))))
                        )
                    ),
                    bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;

                    __m256i v_i = _mm256_set1_epi16(first.data[i]);
                    bitmask = _mm256_or_si256(bitmask, _mm256_cmpeq_epi16(second_data, v_i));
                    has_mask = _mm256_testc_si256(bitmask, ones);
                }
            }
        }
    }

    if (!has_mask && second.size > 15)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

// SSE Int8, UInt8 specialization
// Int8 uses SSE rather than AVX2 because with 16 elements per register we need 16 shuffle rotations,
// which already covers all combinations. AVX2 would need cross-lane shuffles for 32 elements,
// making it more complex without clear benefit.
template<typename IntType>
requires (std::is_same_v<IntType, Int8> || std::is_same_v<IntType, UInt8>)
NO_INLINE bool sliceHasImplAnyAllImplInt8(
    const NumericArraySlice<IntType> & first,
    const NumericArraySlice<IntType> & second,
    const UInt8 * first_null_map,
    const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    size_t j = 0;
    int has_mask = 1;
    static constexpr int8_t full = -1;
    static constexpr int8_t none = 0;
    const __m128i zeros = _mm_setzero_si128();

    if (second.size > 15 && first.size > 15)
    {
        for (; j < second.size - 15 && has_mask; j += 16)
        {
            has_mask = 0;
            const __m128i second_data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(second.data + j));
            __m128i bitmask = has_second_null_map ?
                _mm_set_epi8(
                    (second_null_map[j + 15]) ? full : none, (second_null_map[j + 14]) ? full : none,
                    (second_null_map[j + 13]) ? full : none, (second_null_map[j + 12]) ? full : none,
                    (second_null_map[j + 11]) ? full : none, (second_null_map[j + 10]) ? full : none,
                    (second_null_map[j + 9]) ? full : none, (second_null_map[j + 8]) ? full : none,
                    (second_null_map[j + 7]) ? full : none, (second_null_map[j + 6]) ? full : none,
                    (second_null_map[j + 5]) ? full : none, (second_null_map[j + 4]) ? full : none,
                    (second_null_map[j + 3]) ? full : none, (second_null_map[j + 2]) ? full : none,
                    (second_null_map[j + 1]) ? full : none, (second_null_map[j]) ? full : none)
                : zeros;

            size_t i = 0;
            for (; i < first.size - 15 && !has_mask; has_mask = _mm_test_all_ones(bitmask), i += 16)
            {
                const __m128i first_data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(first.data + i));
                const __m128i first_nm_mask = has_first_null_map ?
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(first_null_map + i))
                    : zeros;
                bitmask =
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            first_nm_mask,
                                            _mm_cmpeq_epi8(second_data, first_data)),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,15)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,15))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(13,12,11,10,9,8,7,6,5,4,3,2,1,0,15,14)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(12,11,10,9,8,7,6,5,4,3,2,1,0,15,14,13)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(12,11,10,9,8,7,6,5,4,3,2,1,0,15,14,13)))))
                                ),
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(11,10,9,8,7,6,5,4,3,2,1,0,15,14,13,12)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(10,9,8,7,6,5,4,3,2,1,0,15,14,13,12,11)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(10,9,8,7,6,5,4,3,2,1,0,15,14,13,12,11))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(9,8,7,6,5,4,3,2,1,0,15,14,13,12,11,10)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(8,7,6,5,4,3,2,1,0,15,14,13,12,11,10,9)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(8,7,6,5,4,3,2,1,0,15,14,13,12,11,10,9))))))),
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(7,6,5,4,3,2,1,0,15,14,13,12,11,10,9,8)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(6,5,4,3,2,1,0,15,14,13,12,11,10,9,8,7)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(6,5,4,3,2,1,0,15,14,13,12,11,10,9,8,7))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(5,4,3,2,1,0,15,14,13,12,11,10,9,8,7,6)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(4,3,2,1,0,15,14,13,12,11,10,9,8,7,6,5)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(4,3,2,1,0,15,14,13,12,11,10,9,8,7,6,5)))))),
                                _mm_or_si128(
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(3,2,1,0,15,14,13,12,11,10,9,8,7,6,5,4)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(2,1,0,15,14,13,12,11,10,9,8,7,6,5,4,3)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(2,1,0,15,14,13,12,11,10,9,8,7,6,5,4,3))))),
                                    _mm_or_si128(
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(1,0,15,14,13,12,11,10,9,8,7,6,5,4,3,2)))),
                                        _mm_andnot_si128(
                                            _mm_shuffle_epi8(first_nm_mask, _mm_set_epi8(0,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)),
                                            _mm_cmpeq_epi8(second_data, _mm_shuffle_epi8(first_data, _mm_set_epi8(0,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)))))))),
                        bitmask);
            }

            if (i < first.size)
            {
                for (; i < first.size && !has_mask; ++i)
                {
                    if (has_first_null_map && first_null_map[i])
                        continue;

                    __m128i v_i = _mm_set1_epi8(first.data[i]);
                    bitmask = _mm_or_si128(bitmask, _mm_cmpeq_epi8(second_data, v_i));
                    has_mask = _mm_test_all_ones(bitmask);
                }
            }
        }
    }

    if (!has_mask && second.size > 15)
        return false;

    return hasAllIntegralLoopRemainder(j, first, second, first_null_map, second_null_map);
}

#endif

#if USE_ARM_MULTITARGET_CODE
DECLARE_ARM_SVE_SPECIFIC_CODE(

/// Maps an integer type to its SVE vector type, null-map vector type, and scalar type.
template <typename T> struct SVEVecTypes;
template <> struct SVEVecTypes<Int8>   { using Vec = svint8_t;   using NullVec = svuint8_t;  using Scalar = int8_t;   };
template <> struct SVEVecTypes<UInt8>  { using Vec = svuint8_t;  using NullVec = svuint8_t;  using Scalar = uint8_t;  };
template <> struct SVEVecTypes<Int16>  { using Vec = svint16_t;  using NullVec = svuint16_t; using Scalar = int16_t;  };
template <> struct SVEVecTypes<UInt16> { using Vec = svuint16_t; using NullVec = svuint16_t; using Scalar = uint16_t; };
template <> struct SVEVecTypes<Int32>  { using Vec = svint32_t;  using NullVec = svuint32_t; using Scalar = int32_t;  };
template <> struct SVEVecTypes<UInt32> { using Vec = svuint32_t; using NullVec = svuint32_t; using Scalar = uint32_t; };
template <> struct SVEVecTypes<Int64>  { using Vec = svint64_t;  using NullVec = svuint64_t; using Scalar = int64_t;  };
template <> struct SVEVecTypes<UInt64> { using Vec = svuint64_t; using NullVec = svuint64_t; using Scalar = uint64_t; };

/// SVE intrinsics used by sliceHasImplAnyAllImplSVE, selected by sizeof(T).
template <typename T>
struct SliceHasSVETraits
{
    using Vec = typename SVEVecTypes<T>::Vec;
    using NullVec = typename SVEVecTypes<T>::NullVec;
    using Scalar = typename SVEVecTypes<T>::Scalar;

    /// Predicate for lanes in [pos, size).
    static ALWAYS_INLINE svbool_t whilelt(size_t pos, size_t size)
    {
        if constexpr (sizeof(T) == 1)
            return svwhilelt_b8(UInt64(pos), UInt64(size));
        if constexpr (sizeof(T) == 2)
            return svwhilelt_b16(UInt64(pos), UInt64(size));
        if constexpr (sizeof(T) == 4)
            return svwhilelt_b32(UInt64(pos), UInt64(size));
        if constexpr (sizeof(T) == 8)
            return svwhilelt_b64(UInt64(pos), UInt64(size));
    }

    /// Elements per SVE register.
    static ALWAYS_INLINE size_t lanes()
    {
        if constexpr (sizeof(T) == 1)
            return svcntb();
        if constexpr (sizeof(T) == 2)
            return svcnth();
        if constexpr (sizeof(T) == 4)
            return svcntw();
        if constexpr (sizeof(T) == 8)
            return svcntd();
    }

    static ALWAYS_INLINE Vec load(svbool_t pg, const T * ptr)
    {
        return svld1(pg, reinterpret_cast<const Scalar *>(ptr));
    }

    static ALWAYS_INLINE svbool_t cmpeq(svbool_t pg, Vec data, T value)
    {
        return svcmpeq(pg, data, static_cast<Scalar>(value));
    }

    /// Predicate marking lanes whose element is null.
    static ALWAYS_INLINE svbool_t loadNullMask(svbool_t pg, const UInt8 * ptr)
    {
        const auto * p = reinterpret_cast<const uint8_t *>(ptr);
        NullVec nulls;
        if constexpr (sizeof(T) == 1)
            nulls = svld1(pg, p);
        if constexpr (sizeof(T) == 2)
            nulls = svld1ub_u16(pg, p);
        if constexpr (sizeof(T) == 4)
            nulls = svld1ub_u32(pg, p);
        if constexpr (sizeof(T) == 8)
            nulls = svld1ub_u64(pg, p);
        return svcmpne(pg, nulls, 0);
    }
};

/// SVE ArraySearchType::All: true if every non-null element of `second` is in `first`.
/// Outer loop hand-unrolled 4x: each scan of `first` tests 4 needles from `second` at once.
template <typename IntType>
requires (
    std::is_same_v<IntType, Int8> || std::is_same_v<IntType, UInt8>
    || std::is_same_v<IntType, Int16> || std::is_same_v<IntType, UInt16>
    || std::is_same_v<IntType, Int32> || std::is_same_v<IntType, UInt32>
    || std::is_same_v<IntType, Int64> || std::is_same_v<IntType, UInt64>)
NO_INLINE bool sliceHasImplAnyAllImplSVE(
    const NumericArraySlice<IntType> & first,
    const NumericArraySlice<IntType> & second,
    const UInt8 * first_null_map,
    const UInt8 * second_null_map)
{
    if (second.size == 0)
        return true;

    if (!hasNull(first_null_map, first.size) && hasNull(second_null_map, second.size))
        return false;

    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    using Traits = SliceHasSVETraits<IntType>;
    const size_t lanes = Traits::lanes();
    size_t j = 0;

    for (; j + 3 < second.size; j += 4)
    {
        const IntType needle0 = second.data[j];
        const IntType needle1 = second.data[j + 1];
        const IntType needle2 = second.data[j + 2];
        const IntType needle3 = second.data[j + 3];

        bool found0 = has_second_null_map && second_null_map[j];
        bool found1 = has_second_null_map && second_null_map[j + 1];
        bool found2 = has_second_null_map && second_null_map[j + 2];
        bool found3 = has_second_null_map && second_null_map[j + 3];

        for (size_t i = 0; i < first.size; i += lanes)
        {
            const svbool_t pg = Traits::whilelt(i, first.size);
            svbool_t pg_valid = pg;
            const auto first_vec = Traits::load(pg, first.data + i);

            if (has_first_null_map)
                pg_valid = svbic_b_z(pg, pg, Traits::loadNullMask(pg, first_null_map + i));

            if (!found0)
                found0 = svptest_any(pg_valid, Traits::cmpeq(pg_valid, first_vec, needle0));

            if (!found1)
                found1 = svptest_any(pg_valid, Traits::cmpeq(pg_valid, first_vec, needle1));

            if (!found2)
                found2 = svptest_any(pg_valid, Traits::cmpeq(pg_valid, first_vec, needle2));

            if (!found3)
                found3 = svptest_any(pg_valid, Traits::cmpeq(pg_valid, first_vec, needle3));

            if (found0 && found1 && found2 && found3)
                break;
        }

        if (!(found0 && found1 && found2 && found3))
            return false;
    }

    /// Tail: leftover elements of `second`.
    for (; j < second.size; ++j)
    {
        if (has_second_null_map && second_null_map[j])
            continue;

        const IntType needle = second.data[j];
        bool found = false;

        for (size_t i = 0; i < first.size; i += lanes)
        {
            const svbool_t pg = Traits::whilelt(i, first.size);
            svbool_t pg_valid = pg;
            const auto first_vec = Traits::load(pg, first.data + i);

            if (has_first_null_map)
                pg_valid = svbic_b_z(pg, pg, Traits::loadNullMask(pg, first_null_map + i));

            const svbool_t match = Traits::cmpeq(pg_valid, first_vec, needle);
            if (svptest_any(pg_valid, match))
            {
                found = true;
                break;
            }
        }

        if (!found)
            return false;
    }

    return true;
}
) /// DECLARE_ARM_SVE_SPECIFIC_CODE

#endif

template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
    bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
bool sliceHasImplAnyAllGenericImpl(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    const bool has_second_null = hasNull(second_null_map, second.size);
    if (has_second_null)
    {
        const bool has_first_null = hasNull(first_null_map, first.size);

        if (has_first_null && search_type == ArraySearchType::Any)
            return true;

        if (!has_first_null && search_type == ArraySearchType::All)
            return false;
    }

    for (size_t i = 0; i < second.size; ++i)
    {
        if (has_second_null_map && second_null_map[i])
            continue;

        bool has = false;

        for (size_t j = 0; j < first.size && !has; ++j)
        {
            if (has_first_null_map && first_null_map[j])
                continue;

            if (isEqual(first, second, j, i))
            {
                has = true;
                break;
            }
        }

        if (has && search_type == ArraySearchType::Any)
            return true;

        if (!has && search_type == ArraySearchType::All)
            return false;
    }

    return search_type == ArraySearchType::All;
}

/// Methods to check if first array has elements from second array, overloaded for various combinations of types.
template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
    bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
inline ALWAYS_INLINE bool sliceHasImplAnyAll(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if constexpr (search_type == ArraySearchType::All && std::is_same_v<FirstSliceType, SecondSliceType>)
    {
#if defined(__AVX2__)
        if constexpr (std::is_same_v<FirstSliceType, NumericArraySlice<Int8>> || std::is_same_v<FirstSliceType, NumericArraySlice<UInt8>>)
        {
            return sliceHasImplAnyAllImplInt8(first, second, first_null_map, second_null_map);
        }
        else if constexpr (std::is_same_v<FirstSliceType, NumericArraySlice<Int16>> || std::is_same_v<FirstSliceType, NumericArraySlice<UInt16>>)
        {
            return sliceHasImplAnyAllImplInt16(first, second, first_null_map, second_null_map);
        }
        else if constexpr (std::is_same_v<FirstSliceType, NumericArraySlice<Int32>> || std::is_same_v<FirstSliceType, NumericArraySlice<UInt32>>)
        {
            return sliceHasImplAnyAllImplInt32(first, second, first_null_map, second_null_map);
        }
        else if constexpr (std::is_same_v<FirstSliceType, NumericArraySlice<Int64>> || std::is_same_v<FirstSliceType, NumericArraySlice<UInt64>>)
        {
            return sliceHasImplAnyAllImplInt64(first, second, first_null_map, second_null_map);
        }
#elif USE_ARM_MULTITARGET_CODE
    if (isArchSupported(TargetArch::ARM_SVE))
        {
            if constexpr (
                std::is_same_v<FirstSliceType, NumericArraySlice<Int8>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<UInt8>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<Int16>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<UInt16>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<Int32>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<UInt32>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<Int64>> ||
                std::is_same_v<FirstSliceType, NumericArraySlice<UInt64>>)
            {
                return TargetSpecific::ARM_SVE::sliceHasImplAnyAllImplSVE(first, second, first_null_map, second_null_map);
            }
        }
#endif
    }

    return sliceHasImplAnyAllGenericImpl<search_type, FirstSliceType, SecondSliceType, isEqual>(first, second, first_null_map, second_null_map);
}


}
