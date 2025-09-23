#pragma once

#include <Common/HashTable/HashTableKeyHolder.h>
#include <Columns/IColumn.h>

namespace DB
{
struct Settings;

template <bool is_plain_column = false>
static auto getKeyHolder(const IColumn & column, size_t row_num, Arena & arena)
{
    if constexpr (is_plain_column)
    {
        return ArenaKeyHolder{column.getDataAt(row_num), arena};
    }
    else
    {
        const char * begin = nullptr;
        auto serialized = column.serializeAggregationStateValueIntoArena(row_num, arena, begin);
        chassert(serialized.data() != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserializeAndInsert(std::string_view str, IColumn & data_to)
{
    if constexpr (is_plain_column)
        data_to.insertData(str.data(), str.size());
    else
        std::ignore = data_to.serializeAggregationStateValueIntoArena(str.data()); /// NOLINT(bugprone-suspicious-stringview-data-usage)
}

}
