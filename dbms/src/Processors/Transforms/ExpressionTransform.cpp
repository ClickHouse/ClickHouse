#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

static Block transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    expression->execute(header, true);
    return header;
}


ExpressionTransform::ExpressionTransform(const Block & header, ExpressionActionsPtr expression, bool on_totals, bool default_totals)
    : ISimpleTransform(header, transformHeader(header, expression), on_totals)
    , expression(std::move(expression))
    , on_totals(on_totals)
    , default_totals(default_totals)
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (on_totals)
    {
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
