#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorBitmap.h>
#include <Common/Exception.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

namespace
{

/// Span below which the upstream filter is summed by hand rather than through the vectorised
/// `countBytesInFilter`. A dense deletion vector leaves only a byte or two between consecutive
/// deleted rows, and over such a span the call costs more than the work it does.
constexpr size_t min_span_to_vectorise = 64;

/// Drops the rows of `chunk` whose row numbers belong to the deletion set.
///
/// The row numbers covered by a chunk form a contiguous range, so instead of asking the deletion
/// set about every row, we ask it for the deleted row numbers falling into that range. `enumerate`
/// must call its callback with every such row number, in ascending order and without duplicates.
template <typename Enumerate>
void applyDeletedRows(DB::Chunk & chunk, Enumerate && enumerate)
{
    auto chunk_info = chunk.getChunkInfos().get<DB::ChunkInfoRowNumbers>();
    if (!chunk_info)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumbers does not exist");

    const size_t num_rows_before = chunk.getNumRows();
    if (num_rows_before == 0)
        return;

    auto & applied_filter = chunk_info->applied_filter;
    const size_t num_indices = applied_filter.has_value() ? applied_filter->size() : num_rows_before;
    chassert(!applied_filter.has_value() || countBytesInFilter(*applied_filter) == num_rows_before);

    const UInt64 range_begin = chunk_info->row_num_offset;

    DB::IColumn::Filter filter;
    size_t num_rows_after = num_rows_before;
    /// Index within the range up to which `num_surviving_before` has already been accounted for.
    size_t scanned = 0;
    /// Number of rows of the range before `scanned` that are present in the chunk.
    size_t num_surviving_before = 0;

    enumerate(range_begin, range_begin + num_indices, [&](UInt64 row_num)
    {
        const size_t i = row_num - range_begin;
        /// Row numbers are consecutive, so an index within the range is also an index in the chunk,
        /// unless rows were already dropped upstream.
        size_t index_in_chunk = i;

        if (applied_filter.has_value())
        {
            auto & mask = applied_filter.value();
            /// The row is not in the chunk: it was filtered out upstream.
            if (!mask[i])
                return;

            /// Rows that survived between the previously handled row number and this one.
            if (i - scanned < min_span_to_vectorise)
            {
                for (size_t j = scanned; j < i; ++j)
                    num_surviving_before += mask[j] != 0;
            }
            else
                num_surviving_before += countBytesInFilter(mask.data(), scanned, i);
            index_in_chunk = num_surviving_before;

            /// If we already have a _row_number-indexed filter vector, update it in place.
            mask[i] = 0;

            ++num_surviving_before;
            scanned = i + 1;
        }

        /// Allocating lazily keeps chunks with nothing to delete from paying for it.
        if (filter.empty())
            filter.assign(num_rows_before, static_cast<UInt8>(1));

        filter[index_in_chunk] = 0;
        --num_rows_after;
    });

    if (num_rows_after == num_rows_before)
        return;

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, static_cast<ssize_t>(num_rows_after));

    /// If it's the first filtering we do on this Chunk, assign its applied_filter.
    if (!applied_filter.has_value())
        applied_filter.emplace(std::move(filter));

    chunk.setColumns(std::move(columns), num_rows_after);
}

}

DeletionVectorTransform::DeletionVectorTransform(
    const DB::SharedHeader & header_,
    ExcludedRowsPtr excluded_rows_)
    : ISimpleTransform(header_, header_, /* skip_empty_chunks */false)
    , excluded_rows(std::move(excluded_rows_))
{
}

void DeletionVectorTransform::transform(DB::Chunk & chunk)
{
    transform(chunk, *excluded_rows);
}

void DeletionVectorTransform::transform(DB::Chunk & chunk, const ExcludedRows & excluded_rows)
{
    applyDeletedRows(chunk, [&](UInt64 range_begin, UInt64 range_end, auto && callback)
    {
        excluded_rows.forEachInRange(range_begin, range_end, callback);
    });
}

}
