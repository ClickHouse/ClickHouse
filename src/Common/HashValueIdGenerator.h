#pragma once
#include <functional>
#include <type_traits>
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashMap.h>
#include "Exception.h"

#if defined(__AVX512F__) && defined(__AVX512BW__)
#include <immintrin.h>
#endif

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}
namespace ColumnsHashing
{
class HashMethodContext;
}

class IHashValueIdGenerator
{
public:
    explicit IHashValueIdGenerator(const IColumn * /*col_*/, AdaptiveKeysHolder::State *state_, size_t max_distinct_values_)
        : state(state_), max_distinct_values(max_distinct_values_)
    {
    }
    virtual ~IHashValueIdGenerator() = default;

    void computeValueId(const IColumn * col, UInt64 * value_ids)
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
            return;

        if (enable_range_mode && (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID || m_value_ids.size() > range_max - range_min + 1))
        {
            for (UInt64 i = 0, n = range_max - range_min + 1; i < n; ++i)
            {
                StringRef raw_value(reinterpret_cast<const UInt8 *>(&i), sizeof(UInt64));
                emplaceValueId(raw_value, i);
            }
            enable_range_mode = false;
#if defined(__AVX512F__) && defined(__AVX512BW__)
            enable_cache_line = false;
#endif
        }

        computeValueIdImpl(col, value_ids);

#if defined(__AVX512F__) && defined(__AVX512BW__)
        if (!enable_range_mode && enable_cache_line && m_value_ids.size() > cache_line_num * 16)
        {
            enable_cache_line = false;
        }
#endif
        if (allocated_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
            m_value_ids.clearAndShrink();
        }
    }

    void release()
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
        {
            m_value_ids.clearAndShrink();
        }
    }

protected:
    AdaptiveKeysHolder::State *state;
    size_t max_distinct_values;

    UInt64 range_min = -1UL;
    UInt64 range_max = 0;
    bool enable_range_mode = false;
    bool is_nullable = false;

    const size_t max_sample_rows = 10000;


    struct ValueIdCacheLine
    {
        size_t allocated_num = 0;
        alignas(64) UInt64 values[8] = {0};
        alignas(64) UInt64 value_ids[8] = {0};
    };
    using MAP= StringHashMap<UInt64>;
    MAP m_value_ids;
    UInt64 allocated_value_id = 0;
#if defined(__AVX512F__) && defined(__AVX512BW__)
    static constexpr size_t cache_line_num = 1;
    static constexpr size_t cache_line_num_mask = cache_line_num - 1;
    ValueIdCacheLine value_ids_cache_line[cache_line_num];
    bool enable_cache_line = true;
#endif

    virtual void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) = 0;

    /// To use StringHashMap, need to make the memory address padded.
    static constexpr size_t pad_right = integerRoundUp(PADDING_FOR_SIMD - 1, 1);
    static constexpr size_t pad_left = integerRoundUp(PADDING_FOR_SIMD, 1);
    Arena pool;

    // default implementation.
    // The address may be not aligned. Cannot cast the pointer into a integer pointer.
    static constexpr int DYNAMIC_STR_LEN = 0;
    template<int len_type>
    ALWAYS_INLINE UInt64 str2Int64(const UInt8 * pos, size_t len)
    {
        assert(len <= 8);
        if (len == 1)
        {
            return static_cast<UInt64>(*reinterpret_cast<const UInt8*>(pos));
        }
        else if (len == 2)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            return res;
        }
        else if (len == 4)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            res |= (static_cast<UInt64>(pos[3]) << 24);
            return res;
        }
        else if (len == 8)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            res |= (static_cast<UInt64>(pos[3]) << 24);
            res |= (static_cast<UInt64>(pos[4]) << 32);
            res |= (static_cast<UInt64>(pos[5]) << 40);
            res |= (static_cast<UInt64>(pos[6]) << 48);
            res |= (static_cast<UInt64>(pos[7]) << 56);
            return res;
        }
        else if (len == 3)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            return res;
        }
        else if (len == 5)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            res |= (static_cast<UInt64>(pos[3]) << 24);
            res |= (static_cast<UInt64>(pos[4]) << 32);
            return res;

        }
        else  if (len == 6)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            res |= (static_cast<UInt64>(pos[3]) << 24);
            res |= (static_cast<UInt64>(pos[4]) << 32);
            res |= (static_cast<UInt64>(pos[5]) << 40);
            return res;
        }
        else if (len == 7)
        {
            auto res = static_cast<UInt64>(pos[0]);
            res |= (static_cast<UInt64>(pos[1]) << 8);
            res |= (static_cast<UInt64>(pos[2]) << 16);
            res |= (static_cast<UInt64>(pos[3]) << 24);
            res |= (static_cast<UInt64>(pos[4]) << 32);
            res |= (static_cast<UInt64>(pos[5]) << 40);
            res |= (static_cast<UInt64>(pos[6]) << 48);
            return res;
        }
        else if (!len)
            return 0;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid string length: {}", len);
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<1>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        return static_cast<UInt64>(*reinterpret_cast<const UInt8*>(pos));
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<2>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        return static_cast<UInt64>(*reinterpret_cast<const UInt16*>(pos));
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<3>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        auto res = static_cast<UInt64>(pos[0]);
        res |= (static_cast<UInt64>(pos[1]) << 8);
        res |= (static_cast<UInt64>(pos[2]) << 16);
        return res;
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<4>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        return static_cast<UInt64>(*reinterpret_cast<const UInt32*>(pos));
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<5>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        auto res = static_cast<UInt64>(pos[0]);
        res |= (static_cast<UInt64>(pos[1]) << 8);
        res |= (static_cast<UInt64>(pos[2]) << 16);
        res |= (static_cast<UInt64>(pos[3]) << 24);
        res |= (static_cast<UInt64>(pos[4]) << 32);
        return res;
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<6>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        auto res = static_cast<UInt64>(*reinterpret_cast<const UInt16*>(pos));
        res |= (static_cast<UInt64>(*reinterpret_cast<const UInt16*>(pos + 2)) << 16);
        res |= (static_cast<UInt64>(*reinterpret_cast<const UInt16*>(pos + 4)) << 32);
        return res;
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<7>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        auto res = static_cast<UInt64>(pos[0]);
        res |= (static_cast<UInt64>(pos[1]) << 8);
        res |= (static_cast<UInt64>(pos[2]) << 16);
        res |= (static_cast<UInt64>(pos[3]) << 24);
        res |= (static_cast<UInt64>(pos[4]) << 32);
        res |= (static_cast<UInt64>(pos[5]) << 40);
        res |= (static_cast<UInt64>(pos[6]) << 48);
        return res;
    }
    template<>
    ALWAYS_INLINE UInt64 str2Int64<8>(const UInt8 * pos, size_t len[[maybe_unused]])
    {
        return static_cast<UInt64>(*reinterpret_cast<const UInt64*>(pos));
    }

    ALWAYS_INLINE void emplaceValueId(const StringRef & raw_value, UInt64 value_id)
    {
        MAP::LookupResult m_it;
        bool inserted = false;

        size_t alloc_size = 0;
        if (__builtin_add_overflow(raw_value.size, pad_left + pad_right, &alloc_size))
            throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
        /// need to pad the address.
        auto * new_str = pool.alloc(alloc_size) + pad_left;
        memcpy(new_str, raw_value.data, raw_value.size);

        m_value_ids.emplace(StringRef(new_str, raw_value.size), m_it, inserted);
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE bool getValueIdByCache(UInt64 value [[maybe_unused]], UInt64 & value_id [[maybe_unused]], const StringRef & raw_value [[maybe_unused]])
    {
#if defined(__AVX512F__) && defined(__AVX512BW__)
        if (!enable_cache_line)
            return false;
        auto & cache_line = value_ids_cache_line[value & cache_line_num_mask];
        // If the value id set is too large, give up to check in cache
        if (cache_line.allocated_num > 8)
            return false;
        auto value_v = _mm512_set1_epi64(value);
        auto cached_values = _mm512_loadu_epi64(reinterpret_cast<const UInt8*>(cache_line.values));
        auto cmp_mask = _mm512_cmpeq_epi64_mask(cached_values, value_v);
        auto hit_pos = _tzcnt_u32(cmp_mask);
        if (hit_pos >= 8) [[unlikely]]
        {
            if (cache_line.allocated_num >= 8)
            {
                cache_line.allocated_num += 1;
                return false;
            }
            value_id = allocated_value_id++;
            cache_line.value_ids[cache_line.allocated_num] = value_id;
            cache_line.values[cache_line.allocated_num++] = value;

            emplaceValueId(raw_value, value_id);
            return true;
        }
        else
        {
            value_id = cache_line.value_ids[hit_pos];
            return true;
        }
#endif
        return false;
    }

    ALWAYS_INLINE void getValueIdByMap(const StringRef & raw_value, UInt64 & value_id)
    {
        auto it = m_value_ids.find(raw_value);
        if (it) [[likely]]
        {
            value_id = it->getMapped();
        }
        else
        {
            value_id = allocated_value_id++;
            emplaceValueId(raw_value, value_id);
        }
    }

    ALWAYS_INLINE void getValueId(const StringRef &raw_value, UInt64 serialized_value, UInt64 & value_id)
    {
        if (!getValueIdByCache(serialized_value, value_id, raw_value))
            getValueIdByMap(raw_value, value_id);
    }

    template<int string_lenght_type>
    ALWAYS_INLINE void computeLocalValueIdsForOneBatch(UInt64 * value_ids, size_t n, const UInt8 * data_pos, size_t element_bytes, const UInt8 * null_map)
    {
        if (element_bytes > 8 && enable_range_mode)
        {
            computeLocalValueIdsForOneBatchImpl<true, true, string_lenght_type>(value_ids, n, data_pos, element_bytes, null_map);
        }
        else if (element_bytes > 8 && !enable_range_mode)
        {
            computeLocalValueIdsForOneBatchImpl<false, true, string_lenght_type>(value_ids, n, data_pos, element_bytes, null_map);
        }
        else if (element_bytes <= 8 && enable_range_mode)
        {
            computeLocalValueIdsForOneBatchImpl<true, false, string_lenght_type>(value_ids, n, data_pos, element_bytes, null_map);
        }
        else
        {
            computeLocalValueIdsForOneBatchImpl<false, false, string_lenght_type>(value_ids, n, data_pos, element_bytes, null_map);
        }
    }

    template <bool enable_range_mode, bool is_long_value, int string_lenght_type>
    ALWAYS_INLINE void computeLocalValueIdsForOneBatchImpl(UInt64 * value_ids, size_t n, const UInt8 * data_pos, size_t element_bytes, const UInt8 * null_map)
    {
        UInt64 range_delta = range_min - is_nullable;
        for (size_t i = 0; i < n; ++i)
        {
            if constexpr (is_long_value)
            {
                getValueIdByMap(StringRef(data_pos, element_bytes), value_ids[i]);
            }
            else
            {
                StringRef raw_value(data_pos, element_bytes);
                auto val = str2Int64<string_lenght_type>(data_pos, element_bytes);
                if constexpr (enable_range_mode)
                {
                    if (val > range_max || val < range_min)
                    {
                        getValueId(raw_value, val, value_ids[i]);
                    }
                    else
                    {
                        value_ids[i] = val - range_delta;
                    }
                }
                else
                {
                    getValueId(raw_value, val, value_ids[i]);
                }
            }
            data_pos += element_bytes;
        }
        if (null_map)
        {
            constexpr UInt64 mask = -1UL;
            for (size_t i = 0; i < n; ++i)
            {
                value_ids[i] &= ~(mask * null_map[i]);
            }
        }
    }

#if defined(__AVX512F__) && defined(__AVX512BW__)
    template<int string_lenght_type>
    ALWAYS_INLINE void computeLocalValueIdsForOneBatchAVX512(UInt64 * value_ids, const UInt8 * data_pos, size_t element_bytes, const UInt8 * null_map)
    {
        alignas(64) UInt64 tmp_values[8] = {0};
        const auto * local_data_pos = data_pos;
        for (auto & tmp_value : tmp_values)
        {
            tmp_value = str2Int64<string_lenght_type>(local_data_pos, element_bytes);
            local_data_pos += element_bytes;
        }
        if (enable_range_mode)
        {
            UInt64 range_delta = range_min - is_nullable;
            auto range_max_v = _mm512_set1_epi64(range_max);
            auto range_min_v = _mm512_set1_epi64(range_min);
            __m512i tmp_values_v = _mm512_loadu_epi64(tmp_values);
            auto range_max_cmp_mask = _mm512_cmpgt_epi64_mask(tmp_values_v, range_max_v);
            auto range_min_cmp_mask = _mm512_cmpgt_epi64_mask(range_min_v, tmp_values_v);
            if (!range_max_cmp_mask && !range_min_cmp_mask)
            {
                auto range_delta_v = _mm512_set1_epi64(range_delta);
                tmp_values_v = _mm512_sub_epi64(tmp_values_v, range_delta_v);
                _mm512_storeu_epi64(value_ids, tmp_values_v);
            }
            else
            {
                local_data_pos = data_pos;
                for (size_t j = 0; j < 8; ++j)
                {
                    StringRef raw_value(data_pos, element_bytes);
                    if (tmp_values[j] > range_max || tmp_values[j] < range_min)
                    {
                        getValueId(raw_value, tmp_values[j], value_ids[j]);
                    }
                    else
                    {
                        value_ids[j] = tmp_values[j] - range_delta;
                    }
                    local_data_pos += element_bytes;
                }
            }
        }
        else
        {
            local_data_pos = data_pos;
            for (size_t j = 0; j < 8; ++j)
            {
                StringRef raw_value(data_pos, element_bytes);
                getValueId(raw_value, tmp_values[j], value_ids[j]);
                local_data_pos += element_bytes;
            }
        }
        if (null_map)
        {
            constexpr UInt64 mask = -1UL;
            for (size_t i = 0; i < 8; ++i)
            {
                value_ids[i] &= ~(mask * null_map[i]);
            }
        }

    }

    ALWAYS_INLINE void computeFinalValueIdsOneBatchAVX512(UInt64 * local_value_ids, UInt64 * value_ids) const
    {
        auto multiplier_v = _mm512_set1_epi64(max_distinct_values);
        auto value_ids_v = _mm512_loadu_epi64(value_ids);
        auto multipy_result = _mm512_mullox_epi64(value_ids_v, multiplier_v);
        auto local_value_ids_v = _mm512_loadu_epi64(local_value_ids);
        auto add_result = _mm512_add_epi64(multipy_result, local_value_ids_v);
        _mm512_storeu_epi64(value_ids, add_result);
    }
#endif

    ALWAYS_INLINE void computeFinalValueIdsOneBatch(const UInt64 * local_value_ids, UInt64 * value_ids, size_t n = 8) const
    {
        for (size_t j = 0; j < n; ++j)
        {
            value_ids[j] = value_ids[j] * max_distinct_values + local_value_ids[j];
        }
    }

};


class StringHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit StringHashValueIdGenerator(const IColumn * col_, AdaptiveKeysHolder::State * state_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, max_distinct_values_)
    {
        tryInitialize(col_);
    }

private:
    void tryInitialize(const IColumn * col);

    void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) override
    {
        if (col->empty())
            return;
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }

        const auto * str_col = typeid_cast<const ColumnString *>(nested_col);
        if (!str_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid column: {}", col->getName());
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();
        UInt64 * value_ids_pos = value_ids;

        const auto & offsets = str_col->getOffsets();
        IColumn::Offset prev_offset = 0;

        alignas(64) UInt64 tmp_value_ids[8] = {0};
        alignas(64) UInt64 str_lens[8] = {0};
        size_t i = 0;
        size_t n = str_col->size();

        str_lens[0] = offsets[0];
        const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() : nullptr;
        innerComputeLocalValueIdsForOneBatch(tmp_value_ids, 1, char_pos, str_lens, null_map_pos);
        computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos, 1);
        value_ids_pos += 1;
        char_pos += str_lens[0];
        prev_offset = offsets[0];
        i += 1;

        for (; i + 8 < n; i += 8)
        {
            bool is_all_short_string = false;
            null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            computeOneBatchStringLength(i, offsets, str_lens, is_all_short_string);
#if defined(__AVX512F__) && defined(__AVX512BW__)
            if (is_all_short_string)
            {
                innerComputeLocalValueIdsForOneBatchAVX512(tmp_value_ids, char_pos, str_lens, null_map_pos);
            }
            else
#endif
            {
                innerComputeLocalValueIdsForOneBatch(tmp_value_ids, 8, char_pos, str_lens, null_map_pos);
            }
#if defined(__AVX512F__) && defined(__AVX512BW__)
            computeFinalValueIdsOneBatchAVX512(tmp_value_ids, value_ids_pos);
#else
            computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos);
#endif
            value_ids_pos += 8;
            char_pos += offsets[i + 7] - prev_offset;
            prev_offset = offsets[i + 7];
        }

        if (i < n)
        {
            auto remained = n - i;
            for (size_t j = 0; j < remained; ++j)
            {
                str_lens[j] = offsets[i + j] - prev_offset;
                prev_offset = offsets[i + j];
            }
            null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            innerComputeLocalValueIdsForOneBatch(tmp_value_ids, remained, char_pos, str_lens, null_map_pos);
            computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos, remained);
        }
    }

    // 8 elements in a batch
    ALWAYS_INLINE void computeOneBatchStringLength(size_t start, const IColumn::Offsets & offsets, UInt64 *lens, bool & is_all_short_string[[maybe_unused]])
    {
#if defined(__AVX512F__) && defined(__AVX512BW__)
        const auto * ptr = &offsets[start];
        auto off = _mm512_loadu_epi64(ptr);
        auto prev_off = _mm512_loadu_epi64(ptr - 1);
        auto res = _mm512_sub_epi64(off, prev_off);
        auto max_str_len = _mm512_set1_epi64(9);
        is_all_short_string = _mm512_cmpgt_epi64_mask(res, max_str_len) == 0;
        _mm512_store_epi64(lens, res);
#else
        const auto * offsets_pos = &offsets[start - 1];
        lens[0] = offsets_pos[1] - offsets_pos[0];
        lens[1] = offsets_pos[2] - offsets_pos[1];
        lens[2] = offsets_pos[3] - offsets_pos[2];
        lens[3] = offsets_pos[4] - offsets_pos[3];
        lens[4] = offsets_pos[5] - offsets_pos[4];
        lens[5] = offsets_pos[6] - offsets_pos[5];
        lens[6] = offsets_pos[7] - offsets_pos[6];
        lens[7] = offsets_pos[8] - offsets_pos[7];
#endif
    }

    ALWAYS_INLINE void innerComputeLocalValueIdsForOneBatch(UInt64 * value_ids, size_t n, const UInt8 * data_pos, const UInt64 * element_bytes, const UInt8 * null_map)
    {
        if (enable_range_mode)
        {
            innerComputeLocalValueIdsForOneBatchImpl<true>(value_ids, n, data_pos, element_bytes, null_map);
        }
        else
        {
            innerComputeLocalValueIdsForOneBatchImpl<false>(value_ids, n, data_pos, element_bytes, null_map);
        }
    }

    template <bool enable_range_mode>
    ALWAYS_INLINE void innerComputeLocalValueIdsForOneBatchImpl(UInt64 * value_ids, size_t n, const UInt8 * data_pos, const UInt64 * element_bytes, const UInt8 * null_map)
    {
        UInt64 range_delta = range_min - is_nullable;
        for (size_t i = 0; i < n; ++i)
        {
            if (element_bytes[i] > 9)
            {
                getValueIdByMap(StringRef(data_pos, element_bytes[i] - 1), value_ids[i]);
            }
            else
            {
                StringRef raw_value(data_pos, element_bytes[i] - 1);
                auto val = str2Int64<DYNAMIC_STR_LEN>(data_pos, element_bytes[i] - 1);
                if constexpr (enable_range_mode)
                {
                    if (val > range_max || val < range_min)
                    {
                        getValueId(raw_value, val, value_ids[i]);
                    }
                    else
                    {
                        value_ids[i] = val - range_delta;
                    }
                }
                else
                {
                    getValueId(raw_value, val, value_ids[i]);
                }
            }
            data_pos += element_bytes[i];
        }
        if (null_map)
        {
            constexpr UInt64 mask = -1UL;
            for (size_t i = 0; i < n; ++i)
            {
                value_ids[i] &= ~(mask * null_map[i]);
            }
        }
    }
#if defined(__AVX512F__) && defined(__AVX512BW__)
    ALWAYS_INLINE void innerComputeLocalValueIdsForOneBatchAVX512(UInt64 * value_ids, const UInt8 * data_pos, const UInt64 * element_bytes, const UInt8 * null_map)
    {
        alignas(64) UInt64 tmp_values[8] = {0};
        const auto * local_data_pos = data_pos;
        for (size_t i = 0; i < 8; ++i)
        {
            tmp_values[i] = str2Int64<DYNAMIC_STR_LEN>(local_data_pos, element_bytes[i]);
            local_data_pos += element_bytes[i];
        }
        if (enable_range_mode)
        {
            UInt64 range_delta = range_min - is_nullable;
            auto range_max_v = _mm512_set1_epi64(range_max);
            auto range_min_v = _mm512_set1_epi64(range_min);
            __m512i tmp_values_v = _mm512_loadu_epi64(tmp_values);
            auto range_max_cmp_mask = _mm512_cmpgt_epi64_mask(tmp_values_v, range_max_v);
            auto range_min_cmp_mask = _mm512_cmpgt_epi64_mask(range_min_v, tmp_values_v);
            if (!range_max_cmp_mask && !range_min_cmp_mask)
            {
                auto range_delta_v = _mm512_set1_epi64(range_delta);
                tmp_values_v = _mm512_sub_epi64(tmp_values_v, range_delta_v);
                _mm512_storeu_epi64(value_ids, tmp_values_v);
            }
            else
            {
                local_data_pos = data_pos;
                for (size_t j = 0; j < 8; ++j)
                {
                    StringRef raw_value(local_data_pos, element_bytes[j] - 1);
                    if (tmp_values[j] > range_max || tmp_values[j] < range_min) [[unlikely]]
                    {
                        getValueId(raw_value, tmp_values[j], value_ids[j]);
                    }
                    else
                    {
                        value_ids[j] = tmp_values[j] - range_delta;
                    }
                    local_data_pos += element_bytes[j];
                }
            }
        }
        else
        {
            local_data_pos = data_pos;
            for (size_t j = 0; j < 8; ++j)
            {
                StringRef raw_value(local_data_pos, element_bytes[j] - 1);
                getValueId(raw_value, tmp_values[j], value_ids[j]);
                local_data_pos += element_bytes[j];
            }
        }
        if (null_map)
        {
            constexpr UInt64 mask = -1UL;
            for (size_t i = 0; i < 8; ++i)
            {
                value_ids[i] &= ~(mask * null_map[i]);
            }
        }

    }
#endif
};

template<int string_lenght_type>
class FixedStringHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit FixedStringHashValueIdGenerator(const IColumn *col_, AdaptiveKeysHolder::State *state_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, max_distinct_values_)
    {
        tryInitialize(col_);
    }

private:
    void tryInitialize(const IColumn * col)
    {
        is_nullable = col->isNullable();
        const auto * nested_col = col;
        if (is_nullable)
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
        }
        const auto * str_col = typeid_cast<const ColumnFixedString *>(nested_col);
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();

        enable_range_mode = true;
        auto str_len = str_col->getN();
        if (str_len > 8)
        {
            enable_range_mode = false;
        }
        else
        {
            size_t i = 0;
            size_t n = std::min(max_sample_rows, str_col->size());
            for (i = 0; i < n; i++)
            {
                auto val = str2Int64<string_lenght_type>(char_pos, str_len);
                if (val > range_max)
                    range_max = val;
                if (val < range_min)
                    range_min = val;
                if (range_max - range_min + 1 + is_nullable > max_distinct_values)
                {
                    enable_range_mode = false;
                    break;
                }
                char_pos += str_len;
            }
        }
        enable_range_mode = enable_range_mode && range_max > range_min && range_max - range_min + 1 + is_nullable <= max_distinct_values;
        if (enable_range_mode)
        {
            allocated_value_id = range_max - range_min + 1 + is_nullable;
        }
        else
        {
            allocated_value_id = is_nullable;
        }
    }

    void computeValueIdImpl(const IColumn *col, UInt64 * value_ids) override
    {
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }

        const auto * str_col = typeid_cast<const ColumnFixedString *>(nested_col);
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();
        UInt64 * value_ids_pos = value_ids;
        auto str_len = str_col->getN();

        alignas(64) UInt64 tmp_value_ids[8] = {0};
        size_t i = 0;
        size_t n = str_col->size();

        for (; i + 8 < n; i += 8)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
#if defined(__AVX512F__) && defined(__AVX512BW__)
            if (str_len <= 8)
            {
                computeLocalValueIdsForOneBatchAVX512<string_lenght_type>(tmp_value_ids, char_pos, str_len, null_map_pos);
            }
            else
#endif
            {
                computeLocalValueIdsForOneBatch<string_lenght_type>(tmp_value_ids, 8, char_pos, str_len, null_map_pos);
            }
            /// update the output value_ids.
#if defined(__AVX512F__) && defined(__AVX512BW__)
            computeFinalValueIdsOneBatchAVX512(tmp_value_ids, value_ids_pos);
#else
            computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos);
#endif
            char_pos += str_len * 8;
            value_ids_pos += 8;
        }
        const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
        computeLocalValueIdsForOneBatch<string_lenght_type>(tmp_value_ids, n - i, char_pos, str_len, null_map_pos);
        computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos, n - i);

    }
};

template <typename ElementType, typename ColumnType>
class NumericHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit NumericHashValueIdGenerator(const IColumn * col_, AdaptiveKeysHolder::State * state_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, max_distinct_values_)
    {
        tryInitialize(col_);
    }
private:
    void tryInitialize(const IColumn * col)
    {
        is_nullable = col->isNullable();
        const auto * nested_col = col;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
        }
        const auto * num_col = typeid_cast<const ColumnType *>(nested_col);
        size_t element_bytes = sizeof(ElementType);
        const UInt8 * data_pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        size_t i = 0;
        size_t n = std::min(max_sample_rows, num_col->size());
        for (i = 0; i < n; ++i)
        {
            if (element_bytes > 8)
            {
                enable_range_mode = false;
                break;
            }
            UInt64 val = str2Int64<sizeof(ElementType)>(data_pos, element_bytes);
            if (val > range_max)
                range_max = val;
            if (val < range_min)
                range_min = val;
            if (range_max - range_min + 1 + is_nullable > max_distinct_values)
            {
                enable_range_mode = false;
                break;
            }
            data_pos += element_bytes;
        }
        enable_range_mode = enable_range_mode && range_max > range_min && range_max - range_min + 1 + is_nullable <= max_distinct_values;
        if (enable_range_mode)
        {
            allocated_value_id =  range_max - range_min + 1 + is_nullable;
        }
        else
        {
            allocated_value_id = is_nullable;
        }
    }

    void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) override
    {
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }
        const auto * num_col = typeid_cast<const ColumnType *>(nested_col);
        size_t element_bytes = sizeof(ElementType);
        const UInt8 * data_pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        UInt64 * value_ids_pos = value_ids;
        size_t i = 0;
        size_t n = num_col->size();

        alignas(64) UInt64 tmp_value_ids[8] = {0};
        for (; i + 8 < n; i += 8)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
#if defined(__AVX512F__) && defined(__AVX512BW__)
            if (element_bytes <= 8)
            {
                computeLocalValueIdsForOneBatchAVX512<sizeof(ElementType)>(tmp_value_ids, data_pos, element_bytes, null_map_pos);
            }
            else
#endif
            {
                computeLocalValueIdsForOneBatch<sizeof(ElementType)>(tmp_value_ids, 8, data_pos, element_bytes, null_map_pos);
            }
#if defined(__AVX512F__) && defined(__AVX512BW__)
            computeFinalValueIdsOneBatchAVX512(tmp_value_ids, value_ids_pos);
#else
            computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos);
#endif
            data_pos += element_bytes * 8;
            value_ids_pos += 8;
        }
        const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
        computeLocalValueIdsForOneBatch<sizeof(ElementType)>(tmp_value_ids, n - i, data_pos, element_bytes, null_map_pos);
        computeFinalValueIdsOneBatch(tmp_value_ids, value_ids_pos, n - i);
    }
};

class LowCardinalityHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit LowCardinalityHashValueIdGenerator(const IColumn * col_, AdaptiveKeysHolder::State * state_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, max_distinct_values_)
    {
        enable_range_mode = false;
    }
private:
    void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) override
    {
        const auto * low_card_col = typeid_cast<const ColumnLowCardinality *>(col);
        if (!low_card_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is not LowCardinality", col->getName());
        const auto & indexes_col = low_card_col->getIndexes();
        size_t index_bytes = low_card_col->getSizeOfIndexType();
        const UInt8 * data_pos = nullptr;
        switch (index_bytes)
        {
            case sizeof(UInt8):
            {
                data_pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt8 *>(&indexes_col)->getData().data());
                assignIndexToValueId<UInt8>(value_ids, data_pos, col->size());
                break;
            }
            case sizeof(UInt16):
            {
                data_pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt16 *>(&indexes_col)->getData().data());
                assignIndexToValueId<UInt8>(value_ids, data_pos, col->size());
                break;
            }
            case sizeof(UInt32):
            {
                data_pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt32 *>(&indexes_col)->getData().data());
                assignIndexToValueId<UInt8>(value_ids, data_pos, col->size());
                break;
            }
            case sizeof(UInt64):
            {
                data_pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt64 *>(&indexes_col)->getData().data());
                assignIndexToValueId<UInt8>(value_ids, data_pos, col->size());
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
        }
    }

    template <typename IndexType>
    void assignIndexToValueId(UInt64 * value_ids, const UInt8 * data_pos, size_t n)
    {
        const auto * typed_data_pos = reinterpret_cast<const IndexType *>(data_pos);
        for (size_t i = 0; i < n; ++i)
        {
            UInt64 value_id = typed_data_pos[i];
            value_ids[i] = value_ids[i] * max_distinct_values + value_id;
            data_pos += sizeof(IndexType);
            if (value_id > allocated_value_id)
                allocated_value_id = value_id;
        }
    }
};

class HashValueIdGeneratorFactory
{
public:
    static HashValueIdGeneratorFactory & instance();
    std::unique_ptr<IHashValueIdGenerator> getGenerator(AdaptiveKeysHolder::State *state_, size_t max_distinct_values_, const IColumn * col);
};
}
