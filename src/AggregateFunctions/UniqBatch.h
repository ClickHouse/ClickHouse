#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <array>

namespace DB
{

/// Hash and prefetch a chunk before inserting; retain sets, not cells invalidated by growth.
template <auto insert, typename GetHash, typename GetSet>
void addBatchUniq(size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset,
    const IColumn ** columns, ssize_t if_argument_pos, GetHash get_hash, GetSet get_set)
{
    const auto * flags = if_argument_pos >= 0
        ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
    static constexpr size_t chunk_size = 32;
    std::array<decltype(get_hash(row_begin)), chunk_size> hashes{};
    std::array<decltype(get_set(places[0])), chunk_size> sets{};

    while (row_begin < row_end)
    {
        const size_t chunk_end = row_begin + std::min(chunk_size, row_end - row_begin);
        size_t count = 0;
        for (size_t row = row_begin; row < chunk_end; ++row)
        {
            if (!places[row] || (flags && !flags[row]))
                continue;

            hashes[count] = get_hash(row);
            sets[count++] = get_set(places[row] + place_offset);
        }

        for (size_t i = 0; i < count; ++i)
            sets[i]->prefetch(hashes[i]);
        for (size_t i = 0; i < count; ++i)
            (sets[i]->*insert)(hashes[i]);

        row_begin = chunk_end;
    }
}

}
