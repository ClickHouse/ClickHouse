#include <DataStreams/RollupBlockInputStream.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>

namespace DB
{

static void finalize(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnWithTypeAndName & current = block.getByPosition(i);
        const DataTypeAggregateFunction * unfinalized_type = typeid_cast<const DataTypeAggregateFunction *>(current.type.get());

        if (unfinalized_type)
        {
            current.type = unfinalized_type->getReturnType();
            if (current.column)
                current.column = typeid_cast<const ColumnAggregateFunction &>(*current.column).convertToValues();
        }
    }
}

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
    finalize(res);
    return res;
}


Block RollupBlockInputStream::readImpl()
{
    /** After reading a block from input stream,
      * we will subsequently roll it up on next iterations of 'readImpl'
      * by zeroing out every column one-by-one and re-merging a block.
      */

    if (current_key >= 0)
    {
        auto & current = rollup_block.getByPosition(keys[current_key]);
        current.column = current.column->cloneEmpty()->cloneResized(rollup_block.rows());
        --current_key;

        BlocksList rollup_blocks = { rollup_block };
        rollup_block = aggregator.mergeBlocks(rollup_blocks, false);

        Block finalized = rollup_block;
        finalize(finalized);
        return finalized;
    }

    Block block = children[0]->read();
    current_key = keys.size() - 1;

    rollup_block = block;
    finalize(block);

    return block;
}
}
