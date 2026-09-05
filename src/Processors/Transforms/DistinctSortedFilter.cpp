#include <Processors/Transforms/DistinctSortedFilter.h>

#include <algorithm>
#include <functional>

#include <Columns/ColumnsNumber.h>
#include <Core/SortCursor.h>
#include <Common/assert_cast.h>

namespace DB
{

DistinctSortedFilter::DistinctSortedFilter(ColumnNumbers key_columns_pos_, SortDescription description_, size_t flag_column_pos_)
    : key_columns_pos(std::move(key_columns_pos_))
    , description(std::move(description_))
    , flag_column_pos(flag_column_pos_)
{
    chassert(key_columns_pos.size() == description.size());
    chassert(!key_columns_pos.empty());
}

void DistinctSortedFilter::reset()
{
    prev_chunk_latest_key.clear();
}

void DistinctSortedFilter::saveLatestKey(const ColumnRawPtrs & key_columns, size_t row_pos)
{
    prev_chunk_latest_key.clear();
    for (const auto * col : key_columns)
    {
        prev_chunk_latest_key.emplace_back(col->cloneEmpty());
        prev_chunk_latest_key.back()->insertFrom(*col, row_pos);
    }
}

bool DistinctSortedFilter::isLatestKeyFromPrevChunk(const ColumnRawPtrs & key_columns, size_t row_pos) const
{
    for (size_t i = 0, s = key_columns.size(); i < s; ++i)
    {
        const int res = prev_chunk_latest_key[i]->compareAt(0, row_pos, *key_columns[i], description[i].nulls_direction);
        if (res != 0)
            return false;
    }
    return true;
}

Chunk DistinctSortedFilter::filter(Chunk chunk, bool strip_flag)
{
    const size_t num_rows = chunk.getNumRows();
    if (unlikely(num_rows == 0))
        return chunk;

    auto columns = chunk.detachColumns();
    chassert(flag_column_pos == columns.size() - 1);

    ColumnRawPtrs key_columns;
    key_columns.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        key_columns.emplace_back(columns[pos].get());

    const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[flag_column_pos]).getData();

    IColumn::Filter filter_values(num_rows, 0);
    size_t output_rows = 0;
    size_t range_begin = 0;

    /// If the first row has the same key as the last row of the previous chunk, the previous range
    /// continues into this chunk: it was already decided at its first row, skip the continuation.
    if (!prev_chunk_latest_key.empty() && isLatestKeyFromPrevChunk(key_columns, 0))
        range_begin = getEqualRangeEndAssumeSorted(key_columns, description, 0, num_rows);

    while (range_begin != num_rows)
    {
        const size_t range_end = getEqualRangeEndAssumeSorted(key_columns, description, range_begin, num_rows);

        /// The merge of the runs must return the flagged rows before the equal unflagged ones (the
        /// sorting queues break ties by the input index and the flagged run is the input 0).
        chassert(std::is_sorted(flags.begin() + range_begin, flags.begin() + range_end, std::greater{}));

        /// Keep the first row of the range unless this value was already emitted before the spill.
        if (flags[range_begin] == 0)
        {
            filter_values[range_begin] = 1;
            ++output_rows;
        }

        range_begin = range_end;
    }

    saveLatestKey(key_columns, num_rows - 1);

    if (output_rows != num_rows)
    {
        for (auto & column : columns)
            column = column->filter(filter_values, output_rows);
    }

    if (strip_flag)
        columns.pop_back();

    return Chunk(std::move(columns), output_rows);
}

}
