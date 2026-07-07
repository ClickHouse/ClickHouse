#include <QueryPipeline/scatterByPartition.h>

#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

void scatterByPartition(QueryPipelineBuilder & pipeline, size_t num_partitions, const ColumnNumbers & key_columns, const DataTypes & hash_cast_types)
{
    const size_t num_streams = pipeline.getNumStreams();
    auto stream_header = pipeline.getSharedHeader();

    /// Scatters and resizes are added in one transform call so that the intermediate
    /// num_streams * num_partitions port count does not become the pipe's max_parallel_streams
    /// and inflate the executor thread limit.
    pipeline.transform([&](OutputPortRawPtrs ports)
    {
        chassert(ports.size() == num_streams);

        Processors result;

        /// One scatter per stream; scatter_outputs[stream * num_partitions + partition] is
        /// the output of scatter `stream` that carries the rows of partition `partition`.
        VectorWithMemoryTracking<OutputPort *> scatter_outputs;
        scatter_outputs.reserve(num_streams * num_partitions);
        for (size_t stream = 0; stream < num_streams; ++stream)
        {
            auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, num_partitions, key_columns, hash_cast_types);
            connect(*ports[stream], scatter->getInputs().front());
            for (auto & output : scatter->getOutputs())
                scatter_outputs.push_back(&output);
            result.push_back(std::move(scatter));
        }

        /// For a single stream the scatter alone already produces num_partitions ports in partition order.
        if (num_streams == 1)
            return result;

        /// Merge the num_streams ports of each partition into one with a ResizeProcessor.
        for (size_t partition = 0; partition < num_partitions; ++partition)
        {
            auto resize = std::make_shared<ResizeProcessor>(stream_header, num_streams, 1);
            auto input_it = resize->getInputs().begin();
            for (size_t stream = 0; stream < num_streams; ++stream, ++input_it)
                connect(*scatter_outputs[stream * num_partitions + partition], *input_it);
            result.push_back(std::move(resize));
        }

        return result;
    });

    chassert(pipeline.getNumStreams() == num_partitions);
}

void scatterByPartitionPreservingOrder(
    QueryPipelineBuilder & pipeline,
    size_t num_partitions,
    const ColumnNumbers & key_columns,
    const SortDescription & sort_description,
    size_t max_block_size)
{
    const size_t num_streams = pipeline.getNumStreams();
    auto stream_header = pipeline.getSharedHeader();

    /// Scatters and per-partition merges are added in one transform call so that the intermediate
    /// num_streams * num_partitions port count does not become the pipe's max_parallel_streams
    /// and inflate the executor thread limit.
    pipeline.transform([&](OutputPortRawPtrs ports)
    {
        chassert(ports.size() == num_streams);

        Processors result;

        /// One scatter per stream; scatter_outputs[stream * num_partitions + partition] is
        /// the output of scatter `stream` that carries the rows of partition `partition`.
        /// The scatter preserves the relative order of the rows it routes to a given partition,
        /// so every such output is still sorted by `sort_description`.
        VectorWithMemoryTracking<OutputPort *> scatter_outputs;
        scatter_outputs.reserve(num_streams * num_partitions);
        for (size_t stream = 0; stream < num_streams; ++stream)
        {
            auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, num_partitions, key_columns);
            connect(*ports[stream], scatter->getInputs().front());
            for (auto & output : scatter->getOutputs())
                scatter_outputs.push_back(&output);
            result.push_back(std::move(scatter));
        }

        /// For a single stream the scatter alone already produces num_partitions ports in partition
        /// order, each still sorted - no merge is needed.
        if (num_streams == 1)
            return result;

        /// Merge the num_streams sorted pieces of each partition into one sorted stream. Unlike the
        /// `ResizeProcessor` used by `scatterByPartition`, `MergingSortedTransform` keeps the order.
        for (size_t partition = 0; partition < num_partitions; ++partition)
        {
            auto merge = std::make_shared<MergingSortedTransform>(
                stream_header,
                num_streams,
                sort_description,
                max_block_size,
                /*max_block_size_bytes=*/0,
                /*max_dynamic_subcolumns=*/std::nullopt,
                SortingQueueStrategy::Batch,
                /*limit_=*/0,
                /*always_read_till_end_=*/false,
                /*out_row_sources_buf_=*/nullptr,
                /*filter_column_name_=*/std::nullopt,
                /*use_average_block_sizes=*/false,
                /*apply_virtual_row_conversions=*/false);
            auto input_it = merge->getInputs().begin();
            for (size_t stream = 0; stream < num_streams; ++stream, ++input_it)
                connect(*scatter_outputs[stream * num_partitions + partition], *input_it);
            result.push_back(std::move(merge));
        }

        return result;
    });

    chassert(pipeline.getNumStreams() == num_partitions);
}

}
