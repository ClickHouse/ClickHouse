#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

static Block transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    expression->execute(header, true);
    return header;
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_, bool on_totals_, bool default_totals_)
    : ISimpleTransform(header_, transformHeader(header_, expression_), on_totals_)
    , expression(std::move(expression_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
{
}

void ExpressionTransform::transform(Chunk & chunk)
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

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (on_totals)
    {
        /// Drop totals if both out stream and joined stream doesn't have ones.
        /// See comment in ExpressionTransform.h
        if (default_totals && !expression->hasTotalsInJoin())
            return;

        expression->executeOnTotals(block);
    }
    else
        expression->execute(block);

    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

}
