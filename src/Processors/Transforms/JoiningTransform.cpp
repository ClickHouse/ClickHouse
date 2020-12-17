#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

Block JoiningTransform::transformHeader(Block header, const JoinPtr & join)
{
    ExtraBlockPtr tmp;
    join->joinBlock(header, tmp);
    return header;
}

JoiningTransform::JoiningTransform(Block input_header, JoinPtr join_,
                                   bool on_totals_, bool default_totals_)
    : ISimpleTransform(input_header, transformHeader(input_header, join_), on_totals_)
    , join(std::move(join_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
{}

void JoiningTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        if (join->alwaysReturnsEmptySet() && !on_totals)
        {
            stopReading();
            chunk.clear();
            return;
        }
    }

    Block block;
    if (on_totals)
    {
        /// We have to make chunk empty before return
        block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        /// Drop totals if both out stream and joined stream doesn't have ones.
        /// See comment in ExpressionTransform.h
        if (default_totals && !join->hasTotals())
            return;

        join->joinTotals(block);
    }
    else
        block = readExecute(chunk);

    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

Block JoiningTransform::readExecute(Chunk & chunk)
{
    Block res;

    if (!not_processed)
    {
        if (chunk.hasColumns())
            res = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        if (res)
            join->joinBlock(res, not_processed);
    }
    else if (not_processed->empty()) /// There's not processed data inside expression.
    {
        if (chunk.hasColumns())
            res = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        not_processed.reset();
        join->joinBlock(res, not_processed);
    }
    else
    {
        res = std::move(not_processed->block);
        join->joinBlock(res, not_processed);
    }
    return res;
}

}
