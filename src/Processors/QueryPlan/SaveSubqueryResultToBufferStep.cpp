#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>

#include <Processors/ChunkBuffer.h>
#include <Processors/Transforms/SaveSubqueryResultToBufferTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <cstddef>

namespace DB
{

namespace
{

constexpr ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

}

SaveSubqueryResultToBufferStep::SaveSubqueryResultToBufferStep(
    const SharedHeader & header_,
    ColumnIdentifiers columns_to_save_,
    ChunkBufferPtr chunk_buffer_
) : ITransformingStep(header_, header_, getTraits())
    , columns_to_save(std::move(columns_to_save_))
    , chunk_buffer(std::move(chunk_buffer_))
{}

void SaveSubqueryResultToBufferStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &  /*settings*/)
{
    const auto & input_header = getInputHeaders().front();

    std::vector<size_t> columns_to_save_indices;
    columns_to_save_indices.reserve(columns_to_save.size());
    for (const auto & column : columns_to_save)
    {
        auto index = input_header->getPositionByName(column);
        columns_to_save_indices.push_back(index);
    }

    /// No need to lock here, as this method is called during pipeline building,
    /// before the execution starts.
    chunk_buffer->setInputsNumber(pipeline.getNumStreams());

    /// We only add transforms for the main data streams, not for totals/extremes.
    /// The totals/extremes streams will pass through unchanged.
    /// This avoids the issue where addSimpleTransform would create more transforms
    /// than the number of main streams (because it also creates transforms for
    /// totals and extremes), causing an underflow in the unfinished_inputs counter.
    pipeline.addSimpleTransform(
        [this, &columns_to_save_indices](const SharedHeader & in_header, Pipe::StreamType stream_type)
            -> std::shared_ptr<IProcessor>
        {
            if (stream_type != Pipe::StreamType::Main)
                return nullptr;
            return std::make_shared<SaveSubqueryResultToBufferTransform>(
                in_header,
                chunk_buffer,
                columns_to_save_indices
            );
        });
}

}
