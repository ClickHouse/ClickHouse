#include <Processors/Transforms/ChunkRowRange.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <base/defines.h>

#include <algorithm>

namespace DB
{

UInt64 materializeSlicesIntoChunk(Chunk & chunk, Columns && source_columns, UInt64 source_row_count, const std::vector<ChunkRowRange> & slices)
{
    UInt64 output_row_count = 0;
    for (const auto & slice : slices)
        output_row_count += slice.length;

    chassert(!slices.empty());
    chassert(output_row_count <= source_row_count);
#ifndef NDEBUG
    {
        for (const auto & column : source_columns)
            chassert(column->size() == source_row_count);

        UInt64 previous_slice_end = 0;
        for (const auto & slice : slices)
        {
            chassert(slice.length > 0);
            chassert(slice.start >= previous_slice_end);
            chassert(slice.start + slice.length <= source_row_count);
            previous_slice_end = slice.start + slice.length;
        }
    }
#endif

    const UInt64 first_slice_start = slices.front().start;
    const UInt64 last_slice_end = slices.back().start + slices.back().length;

    if (slices.size() == 1)
    {
        const auto & slice = slices.front();

        /// A single slice keeps the whole chunk, so reuse the source columns.
        if (slice.length == source_row_count)
        {
            chassert(slice.start == 0);
            chunk.setColumns(std::move(source_columns), slice.length);
            return output_row_count;
        }

        Columns output_columns;
        output_columns.reserve(source_columns.size());
        for (const auto & column : source_columns)
            output_columns.push_back(column->cut(slice.start, slice.length));
        chunk.setColumns(std::move(output_columns), slice.length);
        return output_row_count;
    }

    if (output_row_count == source_row_count)
    {
        /// All rows survived, but as multiple slices. Reuse the source columns.
        chunk.setColumns(std::move(source_columns), output_row_count);
        return output_row_count;
    }

    /// Because `slices` are ordered and non-overlapping, if the span from the
    /// first slice start to the last slice end has the same length as the sum
    /// of slice lengths, then the slices have no gaps and form one segment.
    if (last_slice_end - first_slice_start == output_row_count)
    {
        Columns output_columns;
        output_columns.reserve(source_columns.size());
        for (const auto & column : source_columns)
            output_columns.push_back(column->cut(first_slice_start, output_row_count));
        chunk.setColumns(std::move(output_columns), output_row_count);
        return output_row_count;
    }

    /// Kept rows are sparse within the chunk, so build one mask and `filter`.
    IColumn::Filter mask(source_row_count, 0);
    for (const auto & slice : slices)
        std::fill_n(mask.begin() + slice.start, slice.length, UInt8{1});

    Columns output_columns;
    output_columns.reserve(source_columns.size());

    chassert(countBytesInFilter(mask) == output_row_count);

    /// For `ColumnConst`, `filter` would work too, but it would scan the mask
    /// again to count selected rows. We already know `output_row_count`, so use `cut`.
    for (const auto & column : source_columns)
        output_columns.push_back(isColumnConst(*column) ? column->cut(0, output_row_count) : column->filter(mask, output_row_count));
    chunk.setColumns(std::move(output_columns), output_row_count);
    return output_row_count;
}

}
