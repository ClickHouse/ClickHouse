#pragma once
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

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#if defined(__AVX512F__) && defined(__AVX512BW__)
#include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}
namespace ColumnsHashing
{
class HashMethodContext;
}

class HashValueIdGenerator
{
public:
    explicit HashValueIdGenerator(AdaptiveKeysHolder::State * state_, size_t max_distinct_values_ = 256)
        : state(state_), max_distinct_values(max_distinct_values_)
    {
    }
    void computeValueId(const IColumn * col, std::vector<UInt64> & value_ids);
private:
    AdaptiveKeysHolder::State * state;
    const size_t max_distinct_values;
    bool has_initialized = false;
    bool is_nullable = false;
    /// To use StringHashMap, need to make the memory address padded.
    static constexpr size_t pad_right = integerRoundUp(PADDING_FOR_SIMD - 1, 1);
    static constexpr size_t pad_left = integerRoundUp(PADDING_FOR_SIMD, 1);
    Arena pool;

    using MAP= StringHashMap<UInt64>;
    MAP m_value_ids;

    UInt64 current_assigned_value_id = 1;
    #if defined(__AVX512F__) && defined(__AVX512BW__)
    bool could_fitinto_cache = false;
    static constexpr size_t low_cardinality_cache_size = 8;
    size_t low_cardinality_cache_index = 0;
    alignas(64) UInt64 low_cardinality_cache_values[low_cardinality_cache_size] = {0};
    #endif

    void initialize(const IColumn * col);

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

        size_t alloc_size = 0;
        if (__builtin_add_overflow(len, pad_left + pad_right, &alloc_size))
            throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
        /// need to pad the address.
        auto * new_str = pool.alloc(alloc_size) + pad_left;
        memcpy(new_str, pos, len);

        m_value_ids.emplace(StringRef(new_str, len), m_it, inserted);
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE bool tryAssignValueIdInLowCardinalityCache(const UInt64 & value [[maybe_unused]], UInt64 & value_id [[maybe_unused]], const UInt8 * pos [[maybe_unused]], size_t len [[maybe_unused]])
    {
    #if defined(__AVX512F__) && defined(__AVX512BW__)
        if (low_cardinality_cache_index < low_cardinality_cache_size)
        {
            auto value_v = _mm512_set1_epi64(value);
            auto value_line = _mm512_load_epi64(reinterpret_cast<const UInt8 *>(low_cardinality_cache_values));
            // cmp_mask is 8 bits
            auto cmp_mask = _mm512_cmpeq_epi64_mask(value_line, value_v);
            auto cache_pos = _tzcnt_u32(cmp_mask);
            if (cache_pos >= 8) [[unlikely]]
            {
                /// check next cache line
                value_id = low_cardinality_cache_index + 1 + is_nullable;
                low_cardinality_cache_values[low_cardinality_cache_index] = value;
                low_cardinality_cache_index += 1;

                // Also put this value id into the hash map.
                emplaceValueId(pos, len, value_id);

                return true;
            }
            else
            {
                value_id = cache_pos + 1 + is_nullable;
                return true;
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
                    current_col_value_id = 1;
                }
                else
                {
                    if (str_len - 1 <= sizeof(UInt64))
                    {
                        UInt64 value = *reinterpret_cast<const UInt64 *>(pos);
                        value &= (-1UL) >> ((sizeof(UInt64) - str_len + 1) << 3);
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
                    value &= (-1UL) >> ((sizeof(UInt64) - str_len + 1) << 3);
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
                    current_col_value_id = 1;
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
        size_t str_len = col->getN();
        const auto & chars = col->getChars();
        UInt64 * value_id = value_ids.data();
        const UInt8 * pos = chars.data();
#if defined(__AVX512F__) && defined(__AVX512BW__)
        if (could_fitinto_cache)
        {
            auto value_mask =  (-1UL) >> ((sizeof(UInt64) - str_len) << 3);
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
                        UInt64 value = *reinterpret_cast<const UInt64 *>(pos);
                        value &= value_mask;
                        if (!tryAssignValueIdInLowCardinalityCache(value, current_col_value_id, pos, str_len - 1))
                            assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                }
                else
                {
                    UInt64 value = *reinterpret_cast<const UInt64 *>(pos);
                    value &= value_mask;
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
        size_t str_len = sizeof(ElementType);
        const auto * pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        UInt64 * value_id = value_ids.data();

        if (state->hash_mode == AdaptiveKeysHolder::State::VALUE_ID)
        {
#if defined(__AVX512F__) && defined(__AVX512BW__)
            if (could_fitinto_cache)
            {
                UInt64 current_col_value_id = 0;
                auto assign_value = [&]()
                {
                    if constexpr (sizeof(ElementType) < sizeof(UInt64))
                    {
                        if (!tryAssignValueIdInLowCardinalityCache(
                                static_cast<UInt64>(*reinterpret_cast<const ElementType *>(pos)), current_col_value_id, pos, str_len))
                            assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                    else
                    {
                        assignValueIdDynamically(pos, str_len, current_col_value_id);
                    }
                };
                for (size_t i = 0, n = num_col->size(); i < n; ++i)
                {
                    current_col_value_id = 0;
                    if constexpr (is_nullable)
                    {
                        if ((*null_map).getData()[i])
                        {
                            current_col_value_id = 1;
                        }
                        else
                        {
                           assign_value(); 
                        }
                    }
                    else
                    {
                        assign_value();
                    }
                    *value_id = *value_id * max_distinct_values + current_col_value_id;
                    value_id++;
                    pos += str_len;
                }
            }
            else
#endif
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
    }

    void computeValueIdForLowCardinality(const ColumnLowCardinality * col, std::vector<UInt64> & value_ids);
};
}
