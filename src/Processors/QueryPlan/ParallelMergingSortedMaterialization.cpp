#include <Processors/QueryPlan/ParallelMergingSortedMaterialization.h>

#include <Processors/Merges/MaterializeMergedDataTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/BufferChunksTransform.h>
#include <Processors/Transforms/SortChunksBySequenceNumber.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace
{

template <typename Pipeline>
void addParallelMergingSortedMaterializationImpl(
    Pipeline & pipeline,
    size_t materialization_threads,
    size_t max_rows_to_buffer,
    const std::shared_ptr<MergingSortedTransformStats> & stats)
{
    chassert(materialization_threads > 1);
    chassert(stats);

    pipeline.addTransform(std::make_shared<AddSequenceNumber>(pipeline.getSharedHeader()));
    pipeline.resize(materialization_threads);
    pipeline.addSimpleTransform([stats](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != Pipe::StreamType::Main)
            return nullptr;
        return std::make_shared<MaterializeMergedDataTransform>(header, stats);
    });
    pipeline.addSimpleTransform([max_rows_to_buffer](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != Pipe::StreamType::Main)
            return nullptr;

        /// Let each worker materialize one block ahead while an earlier block
        /// from another worker is waiting to be emitted in sequence order.
        return std::make_shared<BufferChunksTransform>(
            header, max_rows_to_buffer, /*max_bytes_to_buffer=*/ 0, /*limit=*/ 0);
    });
    pipeline.addTransform(std::make_shared<SortChunksBySequenceNumber>(pipeline.getHeader(), materialization_threads));
}

}

void addParallelMergingSortedMaterialization(
    Pipe & pipe,
    size_t materialization_threads,
    size_t max_rows_to_buffer,
    const std::shared_ptr<MergingSortedTransformStats> & stats)
{
    addParallelMergingSortedMaterializationImpl(pipe, materialization_threads, max_rows_to_buffer, stats);
}

void addParallelMergingSortedMaterialization(
    QueryPipelineBuilder & pipeline,
    size_t materialization_threads,
    size_t max_rows_to_buffer,
    const std::shared_ptr<MergingSortedTransformStats> & stats)
{
    addParallelMergingSortedMaterializationImpl(pipeline, materialization_threads, max_rows_to_buffer, stats);
}

}
