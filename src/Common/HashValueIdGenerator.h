#pragma once
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashMap.h>
#include <immintrin.h>

namespace DB
{
namespace ColumnsHashing
{
class HashMethodContext;
}

class HashValueIdGenerator
{
public:
    explicit HashValueIdGenerator(AdaptiveKeysHolder::State * state_) : state(state_) {}
    void computeValueId(const IColumn * col, std::vector<UInt64> & value_ids);
private:
    AdaptiveKeysHolder::State * state;
    /// Store the string values for eache value_id. Need to make the string address padded to
    /// insert into the StringHashMap.
    PaddedPODArray<UInt8> pool;
    // Each key will take 1 byte of the final value_id.
    const size_t max_distinct_values= 256;
    using MAP= StringHashMap<UInt64>;
    MAP m_value_ids;
    /// value_id assignment rules:
    /// - 0 is reserved for null values.
    /// - [1, 16] is reserved for short keys. When this is overflow, put keys in following.
    /// - [17, 255] is dynamically assigned for long keys.
    #if defined(__AVX512F__) && defined(__AVX512BW__)
    UInt64 current_assigned_value_id = 65;
    static constexpr size_t low_cardinality_cache_size = 16;
    size_t low_cardinality_cache_index = 0;
    UInt64 low_cardinality_cache_values[low_cardinality_cache_size] = {0};
    #else
    UInt64 current_assigned_value_id = 1;
    #endif

    ALWAYS_INLINE void assignValueIdDynamically(const UInt8 * pos, size_t len, UInt64 & current_col_value_id)
    {
        auto it = m_value_ids.find(StringRef(pos, len));
        if (it) [[likely]]
        {
            current_col_value_id = it->getMapped();
        }
        else
        {
            current_col_value_id = current_assigned_value_id++;
            emplaceValueId(pos, len, current_col_value_id);
        }
    }

    ALWAYS_INLINE void emplaceValueId(const UInt8 * pos, size_t len, const UInt64 & value_id)
    {
        MAP::LookupResult m_it;
        bool inserted = false;
        const size_t old_size = pool.size();
        const size_t size_to_append = len;
        const size_t new_size = old_size + size_to_append;
        pool.resize(new_size);
        const auto * new_str = pool.data() + old_size;
        memcpy(pool.data() + old_size, pos, size_to_append);

        m_value_ids.emplace(StringRef(new_str, len), m_it, inserted);
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE bool tryAssignValueIdInLowCardinalityCache(const UInt64 & value [[maybe_unused]], UInt64 & value_id [[maybe_unused]], const UInt8 * pos [[maybe_unused]], size_t len [[maybe_unused]])
    {
    #if defined(__AVX512F__) && defined(__AVX512BW__)
        if (low_cardinality_cache_index < low_cardinality_cache_size)
        {
            constexpr UInt32 low_cardinality_cache_lines = low_cardinality_cache_size * sizeof(UInt64) / sizeof(__m512i);
            auto value_v = _mm512_set1_epi64(value);
            for (UInt32 i = 0; i < low_cardinality_cache_lines; ++i)
            {
                auto value_line = _mm512_load_epi64(reinterpret_cast<const UInt8 *>(low_cardinality_cache_values) + i * sizeof(__m512i));
                // cmp_mask is 8 bits
                auto cmp_mask = _mm512_cmpeq_epi64_mask(value_v, value_line);
                auto cache_pos = __builtin_ctz(cmp_mask);
                if (cache_pos > 8) [[unlikely]]
                {
                    /// check next cache line
                    if (i < low_cardinality_cache_lines - 1)
                        continue;
                    low_cardinality_cache_values[low_cardinality_cache_index++] = value;
                    value_id = low_cardinality_cache_index;

                    // Also put this value id into the hash map.
                    emplaceValueId(pos, len, value_id);

                    return true;
                }
                else
                {
                    value_id = low_cardinality_cache_values[cache_pos + i * 8];
                    return true;
                }
            }
        }
    #endif
        return false;
    }


    template<bool is_nullable>
    void computeValueIdForShortString(const ColumnUInt8 * null_map, const ColumnString * col, std::vector<UInt64> & value_ids)
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
            return;

        const auto & offsets = col->getOffsets();
        const auto & chars = col->getChars();
        IColumn::Offset prev_offset = 0;
        UInt64 * value_id = value_ids.data();
        const UInt8 * pos = chars.data();
        size_t offset_index = 0;

        for (size_t n = offsets.size(); offset_index < n; ++offset_index)
        {
            auto str_len = offsets[offset_index] - prev_offset;
            UInt64 current_col_value_id = 0;
            if constexpr (is_nullable)
            {
                if ((*null_map).getData()[offset_index])
                {
                    current_col_value_id = 0;
                }
                else
                {
                    if (str_len - 1 <= sizeof(UInt64))
                    {
                        UInt64 value = *reinterpret_cast<const UInt64 *>(pos);
                        value &= (-1UL) >> ((sizeof(UInt64) - str_len + 1) >> 3);
                        if (!tryAssignValueIdInLowCardinalityCache(value, current_col_value_id, pos, str_len - 1))
                            assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
                    }
                    else
                    {
                        assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
                    }
                }
            }
            else
            {
                if (str_len - 1 <= sizeof(UInt64))
                {
                    UInt64 value = *reinterpret_cast<const UInt64 *>(pos);
                    value &= (-1UL) >> ((sizeof(UInt64) - str_len + 1) >> 3);
                    if (!tryAssignValueIdInLowCardinalityCache(value, current_col_value_id, pos, str_len - 1))
                        assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
                }
                else
                {
                    assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
                }
            }
            *value_id = *value_id * max_distinct_values + current_col_value_id;
            value_id++;
            pos += str_len;
            prev_offset = offsets[offset_index];
        }
        if (current_assigned_value_id >= max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    template<bool is_nullable>
    void computeValueIdForString(const ColumnUInt8 * null_map, const ColumnString * col, std::vector<UInt64> & value_ids)
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
            return;
        const auto & offsets = col->getOffsets();
        const auto & chars = col->getChars();
        IColumn::Offset prev_offset = 0;
        UInt64 * value_id = value_ids.data();
        const UInt8 * pos = chars.data();
        size_t offset_index = 0;

        for (size_t n = offsets.size(); offset_index < n; ++offset_index)
        {
            auto str_len = offsets[offset_index] - prev_offset;
            UInt64 current_col_value_id = 0;
            if constexpr (is_nullable)
            {
                if ((*null_map).getData()[offset_index])
                {
                    current_col_value_id = 0;
                }
                else
                {
                    assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
                }
            }
            else
            {
                assignValueIdDynamically(pos, str_len - 1, current_col_value_id);
            }
            *value_id = *value_id * max_distinct_values + current_col_value_id;
            value_id++;
            pos += str_len;
            prev_offset = offsets[offset_index];
        }
        if (current_assigned_value_id >= max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    template<bool is_nullable>
    void computeValueIdForFixedString(const ColumnUInt8 * null_map, const ColumnFixedString * col, std::vector<UInt64> & value_ids)
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
            return;
        if constexpr (is_nullable)
        {
            /// 1 is assigned to null values
            if (current_assigned_value_id == 1)
                current_assigned_value_id = 2;
        }
        size_t str_len = col->getN();
        const auto & chars = col->getChars();
        UInt64 * value_id = value_ids.data();
        const UInt8 * pos = chars.data();
#if defined(__AVX512F__) && defined(__AVX512BW__)
        if (str_len <= 8)
        {
            for (size_t i = 0, n = col->size(); i < n; ++i)
            {
                UInt64 current_col_value_id = 0;
                if constexpr (is_nullable)
                {
                    if ((*null_map).getData()[i])
                    {
                        current_col_value_id = 1;
                    }
                    else
                    {
                        UInt64 value = 0;
                        memcpy(&value, pos, str_len - 1);
                        if (!tryAssignValueIdInLowCardinalityCache(value, current_col_value_id, pos, str_len - 1))
                            assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                }
                else
                {
                    UInt64 value = 0;
                    memcpy(&value, pos, str_len - 1);
                    if (!tryAssignValueIdInLowCardinalityCache(value, current_col_value_id, pos, str_len - 1))
                        assignValueIdDynamically(pos, str_len, current_col_value_id);
                }
                *value_id = *value_id * max_distinct_values + current_col_value_id;
                value_id++;
                pos += str_len;
            }
        }
        else
#endif
        {
            for (size_t i = 0, n = col->size(); i < n; ++i)
            {
                UInt64 current_col_value_id = 0;
                if constexpr (is_nullable)
                {
                    if ((*null_map).getData()[i])
                    {
                        current_col_value_id = 1;
                    }
                    else
                    {
                        assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                }
                else
                {
                    assignValueIdDynamically(pos, str_len, current_col_value_id);
                }
                *value_id = *value_id * max_distinct_values + current_col_value_id;
                value_id++;
                pos += str_len;
            }
        }

        if (current_assigned_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    template<bool is_nullable, typename ElementType, typename ColumnType>
    void computeValueIdForNumber(const ColumnUInt8 * null_map, const ColumnType * num_col, std::vector<UInt64> & value_ids)
    {
        if constexpr (is_nullable)
        {
            /// 1 is assigned to null values
            if (current_assigned_value_id == 1)
                current_assigned_value_id = 2;
        }
        size_t str_len = sizeof(ElementType);
        const auto * pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        UInt64 * value_id = value_ids.data();

        if (state->hash_mode == AdaptiveKeysHolder::State::VALUE_ID)
        {
            for (size_t i = 0, n = num_col->size(); i < n; ++i)
            {
                UInt64 current_col_value_id = 0;
                if constexpr (is_nullable)
                {
                    if ((*null_map).getData()[i])
                    {
                        current_col_value_id = 1;
                    }
                    else
                    {
#if defined(__AVX512F__) && defined(__AVX512BW__)
                        if constexpr (sizeof(ElementType) <= 8)
                        {
                            if (!tryAssignValueIdInLowCardinalityCache(
                                    static_cast<UInt64>(*reinterpret_cast<const ElementType *>(pos)), current_col_value_id, pos, str_len))
                                assignValueIdDynamically(pos, str_len, current_col_value_id);
                        }
                        else
#endif
                        {
                            assignValueIdDynamically(pos, str_len, current_col_value_id);
                        }
                    }
                }
                else
                {
#if defined(__AVX512F__) && defined(__AVX512BW__)
                    if constexpr (sizeof(ElementType) <= 8)
                    {
                        if (!tryAssignValueIdInLowCardinalityCache(
                                static_cast<UInt64>(*reinterpret_cast<const ElementType *>(pos)), current_col_value_id, pos, str_len))
                            assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                    else
#endif
                    {
                        assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                }
                *value_id = *value_id * max_distinct_values + current_col_value_id;
                value_id++;
                pos += str_len;
            }
        }
        if (current_assigned_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    void computeValueIdForLowCardinality(const ColumnLowCardinality * col, std::vector<UInt64> & value_ids);
};
}
