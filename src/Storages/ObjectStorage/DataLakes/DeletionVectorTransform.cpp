#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

DeletionVectorTransform::DeletionVectorTransform(
    const DB::SharedHeader & header_,
    ExcludedRowsPtr excluded_rows_)
    : ISimpleTransform(header_, header_, /* skip_empty_chunks */false)
    , excluded_rows(excluded_rows_)
{
}

void DeletionVectorTransform::transform(DB::Chunk & chunk)
{
    transform(chunk, *excluded_rows);
}

void DeletionVectorTransform::transform(DB::Chunk & chunk, const ExcludedRows & excluded_rows)
{
    auto chunk_info = chunk.getChunkInfos().get<DB::ChunkInfoRowNumbers>();
    if (!chunk_info)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumbers does not exist");

    const size_t num_rows_before = chunk.getNumRows();
    size_t num_rows_after = num_rows_before;
    size_t idx_in_chunk = 0;

    auto & applied_filter = chunk_info->applied_filter;
    const size_t num_indices = applied_filter.has_value() ? applied_filter->size() : num_rows_before;

    DB::IColumn::Filter filter(num_rows_before, true);
    for (size_t i = 0; i < num_indices; i++)
    {
        if (!applied_filter.has_value() || applied_filter.value()[i])
        {
            if (excluded_rows.rb_contains(chunk_info->row_num_offset + i))
            {
                filter[idx_in_chunk] = false;
                --num_rows_after;

                /// If we already have a _row_number-indexed filter vector, update it in place.
                if (applied_filter.has_value())
                    applied_filter.value()[i] = false;
            }
            idx_in_chunk += 1;
        }
    }

    chassert(idx_in_chunk == num_rows_before);
    if (num_rows_after == num_rows_before)
        return;

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, -1);

    /// If it's the first filtering we do on this Chunk, assign its applied_filter.
    if (!applied_filter.has_value())
        applied_filter.emplace(std::move(filter));

    chunk.setColumns(std::move(columns), num_rows_after);
}

}
