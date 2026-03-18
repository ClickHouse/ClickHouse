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
        StringRef serialized = column.serializeValueIntoArena(row_num, arena, begin);
        assert(serialized.data != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserializeAndInsert(StringRef str, IColumn & data_to)
{
    if constexpr (is_plain_column)
        data_to.insertData(str.data, str.size);
    else
        std::ignore = data_to.deserializeAndInsertFromArena(str.data);
}

}
