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
    BlocksList blocks;

    while(1)
    {
        block = children[0]->read();

        if (!block)
            return block;

        Aggregator aggregator(params);

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        Block rollup_block = block;

        for (int i = static_cast<int>(params.keys_size) - 1; i >= 0; --i) 
        {
            auto & current = rollup_block.getByPosition(params.keys[i]);
            current.column = current.column->cloneEmpty()->cloneResized(rollup_block.rows());
            
            BlocksList rollup_blocks = { rollup_block };
            rollup_block = aggregator.mergeBlocks(rollup_blocks, false);
            blocks.push_back(rollup_block);
        }

        finalize(block);
        for (auto & current_block : blocks)
            finalize(current_block);

        for (size_t i = 0; i < block.columns(); ++i)
        {
            MutableColumnPtr column = block.getByPosition(i).column->assumeMutable();
            for (const auto & current_block : blocks)
                column->insertRangeFrom(*current_block.getByPosition(i).column.get(), 0, current_block.rows());
        }

        if (!block)
            continue;

        return block;
    }
}
}
