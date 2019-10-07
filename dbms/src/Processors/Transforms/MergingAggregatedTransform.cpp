#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

MergingAggregatedTransform::MergingAggregatedTransform(
    Block header, AggregatingTransformParamsPtr params, size_t max_threads)
    : IAccumulatingTransform(std::move(header), params->getHeader())
    , params(std::move(params)), max_threads(max_threads)
{
}

void MergingAggregatedTransform::consume(Chunk chunk)
{
    if (!consume_started)
    {
        consume_started = true;
        LOG_TRACE(log, "Reading blocks of partially aggregated data.");
    }

    total_input_rows += chunk.getNumRows();
    ++total_input_blocks;

    auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk in MergingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
    if (!agg_info)
        throw Exception("Chunk should have AggregatedChunkInfo in MergingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    block.info.is_overflows = agg_info->is_overflows;
    block.info.bucket_num = agg_info->bucket_num;

    bucket_to_blocks[agg_info->bucket_num].emplace_back(std::move(block));
}

Chunk MergingAggregatedTransform::generate()
{
    if (!generate_started)
    {
        generate_started = true;
        LOG_TRACE(log, "Read " << total_input_blocks << " blocks of partially aggregated data, total " << total_input_rows
                               << " rows.");

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
