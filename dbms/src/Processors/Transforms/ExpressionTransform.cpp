#include <Processors/Transforms/ExpressionTransform.h>

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
    auto & header = getInputPort().getHeader();
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    if (on_totals)
    {
        if (default_totals && !expression->hasTotalsInJoin())
            return;

        auto block = getInputPort().getHeader().cloneWithColumns(columns);
        expression->executeOnTotals(block);
        num_rows = block.rows();
        columns = block.getColumns();
    }
    else
    {
        expression->execute(header, columns, num_rows, cache);
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
