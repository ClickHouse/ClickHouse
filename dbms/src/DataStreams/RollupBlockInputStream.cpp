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
    const BlockInputStreamPtr & input_, const Aggregator::Params & params_) : params(params_)
{
    children.push_back(input_);
}


Block RollupBlockInputStream::getHeader() const
{
    Block res = children.at(0)->getHeader();
    finalize(res);
    return res;
}


Block RollupBlockInputStream::readImpl()
{
    Block block;

    while(1)
    {
        if (!blocks.empty())
        {
           auto finalized = std::move(blocks.front());
           finalize(finalized);
           blocks.pop_front();
           return finalized; 
        }

        block = children[0]->read();

        if (!block)
            return block;

        Aggregator aggregator(params);

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        Block rollup_block = block;

        for (ssize_t i = params.keys_size - 1; i >= 0; --i) 
        {
            auto & current = rollup_block.getByPosition(params.keys[i]);
            current.column = current.column->cloneEmpty()->cloneResized(rollup_block.rows());
            
            BlocksList rollup_blocks = { rollup_block };
            rollup_block = aggregator.mergeBlocks(rollup_blocks, false);
            blocks.push_back(rollup_block);
        }

        finalize(block);

        if (!block)
            continue;

        return block;
    }
}
}
