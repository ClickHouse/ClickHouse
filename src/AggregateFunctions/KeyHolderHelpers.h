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
        auto settings = IColumn::SerializationSettings::createForAggregationState();
        auto serialized = column.serializeValueIntoArena(row_num, arena, begin, &settings);
        chassert(!serialized.empty());
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserializeAndInsert(std::string_view str, IColumn & data_to)
{
    if constexpr (is_plain_column)
    {
        /// `insertData` of the fixed-size columns ignores the length and always reads the width of
        /// the value, so a shorter element of a crafted state would read past the end of the buffer.
        if (data_to.valuesHaveFixedSize())
        {
            const size_t expected_size = data_to.sizeOfValueIfFixed();
            if (str.size() != expected_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Element of an aggregation state is {} bytes, while {} bytes are expected", str.size(), expected_size);
        }

        data_to.insertData(str.data(), str.size());
    }
    else
    {
        ReadBufferFromString in(str);
        auto settings = IColumn::SerializationSettings::createForAggregationState();
        data_to.deserializeAndInsertFromArena(in, &settings);
        if (!in.eof())
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Extra bytes ({}) found after deserializing aggregation state", in.available());
        }
    }
}

}
