#include <DataStreams/RollupBlockInputStream.h>
#include <DataStreams/finalizeBlock.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>

namespace DB
{

RollupBlockInputStream::RollupBlockInputStream(
    const BlockInputStreamPtr & input_, const Aggregator::Params & params_) : aggregator(params_),
        keys(params_.keys)
{
    children.push_back(input_);
    Aggregator::CancellationHook hook = [this]() { return this->isCancelled(); };
    aggregator.setCancellationHook(hook);
}


Block RollupBlockInputStream::getHeader() const
{
    Block res = children.at(0)->getHeader();
    finalizeBlock(res);
    return res;
}


Block RollupBlockInputStream::readImpl()
{
    /** After reading a block from input stream,
      * we will subsequently roll it up on next iterations of 'readImpl'
      * by zeroing out every column one-by-one and re-merging a block.
      */

    if (!is_data_read)
    {
        BlocksList source_blocks;
        while (auto block = children[0]->read())
            source_blocks.push_back(block);

        if (source_blocks.empty())
            return {};

        is_data_read = true;
        if (source_blocks.size() > 1)
            rollup_block = aggregator.mergeBlocks(source_blocks, false);
        else
            rollup_block = std::move(source_blocks.front());

        current_key = keys.size() - 1;

        auto finalized = rollup_block;
        finalizeBlock(finalized);
        return finalized;
    }

    if (current_key < 0)
        return {};

    auto & current = rollup_block.getByPosition(keys[current_key]);
    current.column = current.column->cloneEmpty()->cloneResized(rollup_block.rows());
    --current_key;

    BlocksList rollup_blocks = { rollup_block };
    rollup_block = aggregator.mergeBlocks(rollup_blocks, false);

    auto finalized = rollup_block;
    finalizeBlock(finalized);
    return finalized;
}
}
