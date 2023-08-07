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
    UInt64 current_assigned_value_id = 1;
    using MAP= StringHashMap<UInt64>;
    MAP m_value_ids;

    ALWAYS_INLINE void assignValueId(const UInt8 * pos, size_t len, UInt64 & current_col_value_id)
    {
        auto it = m_value_ids.find(StringRef(pos, len));
        if (it) [[likely]]
        {
            current_col_value_id = it->getMapped();
        }
        else
        {
            MAP::LookupResult m_it;
            bool inserted = false;
            current_col_value_id = current_assigned_value_id++;

            const size_t old_size = pool.size();
            const size_t size_to_append = len;
            const size_t new_size = old_size + size_to_append;
            pool.resize(new_size);
            const auto * new_str = pool.data() + old_size;
            memcpy(pool.data() + old_size, pos, size_to_append);

            m_value_ids.emplace(StringRef(new_str, len), m_it, inserted);
            m_it->getMapped() = current_col_value_id;
        }
    }

    template<bool is_nullable>
    void computeValueIdForString(const ColumnUInt8 * null_map, const ColumnString * col, std::vector<UInt64> & value_ids)
    {
        const auto & offsets = col->getOffsets();
        const auto & chars = col->getChars();
        IColumn::Offset prev_offset = 0;
        UInt64 * value_id = value_ids.data();
        const UInt8 * pos = chars.data();
        size_t offset_index = 0;
        if constexpr (is_nullable)
        {
            /// 1 is assigned to null values
            if (current_assigned_value_id == 1)
                current_assigned_value_id = 2;
        }

        if (state->hash_mode == AdaptiveKeysHolder::State::VALUE_ID)
        {
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
                        assignValueId(pos, str_len - 1, current_col_value_id);
                    }
                }
                else
                {
                    assignValueId(pos, str_len - 1, current_col_value_id);
                }
                *value_id = *value_id * max_distinct_values + current_col_value_id;
                value_id++;
                pos += str_len;
                prev_offset = offsets[offset_index];
            }
        }
        if (current_assigned_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    template<bool is_nullable>
    void computeValueIdForFixedString(const ColumnUInt8 * null_map, const ColumnFixedString * col, std::vector<UInt64> & value_ids)
    {
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

        if (state->hash_mode == AdaptiveKeysHolder::State::VALUE_ID)
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
                        assignValueId(pos, str_len, current_col_value_id);
                    }
                }
                else
                {
                    assignValueId(pos, str_len, current_col_value_id);
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
                        assignValueId(pos, str_len, current_col_value_id);
                    }
                }
                else
                {
                    assignValueId(pos, str_len, current_col_value_id);
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
