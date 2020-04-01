#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

static Block transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    expression->execute(header, true);
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
    if (likely(!not_processed))
    {
        res = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        if (res)
            expression->execute(res, not_processed, action_number);
    }
    else
    {
        res = std::move(not_processed->block);
        expression->execute(res, not_processed, action_number);
    }
    return res;
}

}
