#pragma once

#include <Columns/IColumn.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

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
        chassert(!serialized.empty());
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserializeAndInsert(std::string_view str, IColumn & data_to)
{
    if constexpr (is_plain_column)
        data_to.insertData(str.data(), str.size());
    else
    {
        ReadBufferFromString in(str);
        data_to.deserializeAndInsertAggregationStateValueFromArena(in);
        if (!in.eof())
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Extra bytes ({}) found after deserializing aggregation state", in.available());
        }
    }
}

}
