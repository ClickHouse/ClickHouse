#include <memory>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/sortBlock.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergingAggregatedTransform::MergingAggregatedTransform(
    Block header_, AggregatingTransformParamsPtr params_, size_t max_threads_, const SelectQueryInfo & query_info_, ContextPtr context_)
    : IAccumulatingTransform(std::move(header_), params_->getHeader())
    , params(std::move(params_)), max_threads(max_threads_), query_info(query_info_), context(context_)
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

    // for (const auto & remote_executor: remote_executors) {
    //     remote_executor->sendQuery(ClientInfo::QueryKind::SECONDARY_QUERY);
    // }
    return chunk;
}

MergingAggregatedOptimizedTransform::MergingAggregatedOptimizedTransform(Block header_, AggregatingTransformParamsPtr params_, SortDescription description_, UInt64 limit_)
    : IProcessor({header_}, {params_->getHeader()})
    , header(header_)
    , params(std::move(params_))
    , description(description_)
    , limit(limit_) {
    }


IProcessor::Status MergingAggregatedOptimizedTransform::prepare()
{
    /// Check can output.
    auto & output = outputs.front();
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (stop_reached)
    {
        if (need_generate)
        {
            return Status::Ready;
        }
        else
        {
            output.push(std::move(top_chunk));
            output.finish();
            input.close();
            return Status::Finished;
        }
    }
    else
    {
        if (is_consume_finished)
        {
            output.push(std::move(top_chunk));
            output.finish();
            return Status::Finished;
        }

        if (input.isFinished())
        {
            is_consume_finished = true;
            return Status::Ready;
        }
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    assert(!is_consume_finished);
    current_chunk = input.pull(true /* set_not_needed */);
    convertToFullIfSparse(current_chunk);
    return Status::Ready;
}

void MergingAggregatedOptimizedTransform::consume(Chunk chunk)
{
    LOG_TRACE(log, "MergingAggregatedOptimizedTransform");
    size_t input_rows = chunk.getNumRows();
    if (!input_rows)
        return;

    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk in MergingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get()))
    {
        auto block = header.cloneWithColumns(chunk.getColumns());
        block.info.is_overflows = agg_info->is_overflows;
        block.info.bucket_num = agg_info->bucket_num;
        blocks_by_levels[agg_info->order_num / 2][agg_info->order_num].push_back(std::move(block));
    }

    if (blocks_by_levels[0][0].size() == 2 && blocks_by_levels[0][1].size() == 2) {
        stop_reached = true;
        need_generate = true;
    }
}

void MergingAggregatedOptimizedTransform::generate()
{
    auto result = params->getHeader().cloneEmpty();

    for (auto & level : blocks_by_levels[0]) {
        auto block = params->aggregator.mergeBlocks(level.second, params->final);
        sortBlock(block, description, limit);
        block = block.cloneWithCutColumns(0, limit);

        for (size_t column_no = 0; column_no < block.columns(); ++column_no) {
            auto col_to = IColumn::mutate(std::move(result.getByPosition(column_no).column));
            const IColumn & col_from = *block.getByPosition(column_no).column.get();
            col_to->insertRangeFrom(col_from, 0, block.rows());
            result.getByPosition(column_no).column = std::move(col_to);
        }
    }

    top_chunk.setColumns(result.getColumns(), result.rows());
    need_generate = false;
}

void MergingAggregatedOptimizedTransform::work()
{
    if (is_consume_finished || need_generate)
    {
        generate();
    }
    else
    {
        consume(std::move(current_chunk));
    }
}

}
