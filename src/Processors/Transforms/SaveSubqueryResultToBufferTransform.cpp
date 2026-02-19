#include <Processors/Transforms/SaveSubqueryResultToBufferTransform.h>

#include <Processors/ChunkBuffer.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

SaveSubqueryResultToBufferTransform::SaveSubqueryResultToBufferTransform(
    SharedHeader header_,
    ChunkBufferPtr chunk_buffer_,
    const std::vector<size_t> & columns_to_save_indices_
)
    : ISimpleTransform(header_, header_, false)
    , chunk_buffer(std::move(chunk_buffer_))
    , columns_to_save_indices(columns_to_save_indices_)
{}

void SaveSubqueryResultToBufferTransform::transform(Chunk & chunk)
{
    QueryPipelineBuilder builder;

    Columns columns_to_save;
    columns_to_save.reserve(columns_to_save_indices.size());
    for (size_t index : columns_to_save_indices)
        columns_to_save.push_back(chunk.getColumns()[index]);

    chunk_buffer->append(Chunk(std::move(columns_to_save), chunk.getNumRows()));
}

void SaveSubqueryResultToBufferTransform::onFinish()
{
    chunk_buffer->onInputFinish();
}

}
