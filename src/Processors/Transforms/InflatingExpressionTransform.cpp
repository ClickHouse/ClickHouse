#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

Block InflatingExpressionTransform::transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    ExtraBlockPtr tmp;
    expression->execute(header, tmp);
    return header;
}

InflatingExpressionTransform::InflatingExpressionTransform(Block input_header, ExpressionActionsPtr expression_,
                                                           bool on_totals_, bool default_totals_)
    : ISimpleTransform(input_header, transformHeader(input_header, expression_), on_totals_)
    , expression(std::move(expression_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
{}

void InflatingExpressionTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        if (expression->resultIsAlwaysEmpty() && !on_totals)
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
        if (default_totals && !expression->hasTotalsInJoin())
            return;

        expression->executeOnTotals(block);
    }
    else
        block = readExecute(chunk);

    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

Block InflatingExpressionTransform::readExecute(Chunk & chunk)
{
    Block res;

    if (!not_processed)
    {
        if (chunk.hasColumns())
            res = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        if (res)
            expression->execute(res, not_processed);
    }
    else if (not_processed->empty()) /// There's not processed data inside expression.
    {
        if (chunk.hasColumns())
            res = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        not_processed.reset();
        expression->execute(res, not_processed);
    }
    else
    {
        res = std::move(not_processed->block);
        expression->execute(res, not_processed);
    }
    return res;
}

}
