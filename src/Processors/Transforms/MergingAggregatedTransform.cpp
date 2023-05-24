#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergingAggregatedTransform::MergingAggregatedTransform(
    Block header_, AggregatingTransformParamsPtr params_, size_t max_threads_)
    : IAccumulatingTransform(std::move(header_), params_->getHeader())
    , params(std::move(params_)), max_threads(max_threads_)
{
}

void MergingAggregatedTransform::consume(Chunk chunk)
{
    if (!consume_started)
    {
        consume_started = true;
        LOG_TRACE(log, "Reading blocks of partially aggregated data.");
    }

    size_t input_rows = chunk.getNumRows();
    if (!input_rows)
        return;

    total_input_rows += input_rows;
    ++total_input_blocks;

    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk in MergingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get()))
    {
        /** If the remote servers used a two-level aggregation method,
        *  then blocks will contain information about the number of the bucket.
        * Then the calculations can be parallelized by buckets.
        * We decompose the blocks to the bucket numbers indicated in them.
        */

        auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        block.info.is_overflows = agg_info->is_overflows;
        block.info.bucket_num = agg_info->bucket_num;

        bucket_to_blocks[agg_info->bucket_num].emplace_back(std::move(block));
    }
    else if (typeid_cast<const ChunkInfoWithAllocatedBytes *>(info.get()))
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        block.info.is_overflows = false;
        block.info.bucket_num = -1;

        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
    }
    else
        throw Exception("Chunk should have AggregatedChunkInfo in MergingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);
}

Chunk MergingAggregatedTransform::generate()
{
    if (!generate_started)
    {
        generate_started = true;
        LOG_DEBUG(log, "Read {} blocks of partially aggregated data, total {} rows.", total_input_blocks, total_input_rows);

        /// Exception safety. Make iterator valid in case any method below throws.
        next_block = blocks.begin();

        /// TODO: this operation can be made async. Add async for IAccumulatingTransform.
        params->aggregator.mergeBlocks(std::move(bucket_to_blocks), data_variants, max_threads);
        blocks = params->aggregator.convertToBlocks(data_variants, params->final, max_threads);
        next_block = blocks.begin();
    }

    if (next_block == blocks.end())
        return {};

    auto block = std::move(*next_block);
    ++next_block;

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    return chunk;
}

}
