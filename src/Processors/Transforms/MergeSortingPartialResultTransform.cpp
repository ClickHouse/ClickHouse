#include <Processors/Transforms/MergeSortingPartialResultTransform.h>

namespace DB
{

MergeSortingPartialResultTransform::MergeSortingPartialResultTransform(
    const Block & header, MergeSortingTransformPtr merge_sorting_transform_,
    UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
    : PartialResultTransform(header, partial_result_limit_, partial_result_duration_ms_)
    , merge_sorting_transform(std::move(merge_sorting_transform_))
    {}

PartialResultTransform::ShaphotResult MergeSortingPartialResultTransform::getRealProcessorSnapshot()
{
    std::lock_guard lock(merge_sorting_transform->snapshot_mutex);
    if (merge_sorting_transform->generated_prefix)
        return {{}, SnaphotStatus::Stopped};

    if (merge_sorting_transform->chunks.empty())
        return {{}, SnaphotStatus::NotReady};

    /// Sort all input data
    merge_sorting_transform->remerge();

    /// It's possible that we had only empty chunks before remerge
    if (merge_sorting_transform->chunks.empty())
        return {{}, SnaphotStatus::NotReady};

    /// Add a copy of the first `partial_result_limit` rows to a generated_chunk
    /// to send it later as a partial result in the next prepare stage of the current processor
    auto generated_columns = merge_sorting_transform->chunks[0].cloneEmptyColumns();

    size_t total_rows = 0;
    for (const auto & merged_chunk : merge_sorting_transform->chunks)
    {
        size_t rows = std::min(merged_chunk.getNumRows(), partial_result_limit - total_rows);
        if (rows == 0)
            break;

        for (size_t position = 0; position < generated_columns.size(); ++position)
        {
            auto column = merged_chunk.getColumns()[position];
            generated_columns[position]->insertRangeFrom(*column, 0, rows);
        }

        total_rows += rows;
    }

    auto partial_result = Chunk(std::move(generated_columns), total_rows, merge_sorting_transform->chunks[0].getChunkInfo());
    merge_sorting_transform->enrichChunkWithConstants(partial_result);
    return {std::move(partial_result), SnaphotStatus::Ready};
}

}
