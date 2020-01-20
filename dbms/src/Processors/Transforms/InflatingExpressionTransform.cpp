#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>

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

void InflatingExpressionTransform::work()
{
    if (current_data.exception)
        return;

    try
    {
        transform(current_data.chunk);
    }
    catch (DB::Exception &)
    {
        current_data.exception = std::current_exception();
        transformed = true;
        has_input = false;
        return;
    }

    /// The only change from ISimpleTransform::work()
    if (!not_processed)
        has_input = false;

    if (!skip_empty_chunks || current_data.chunk)
        transformed = true;

    if (transformed && !current_data.chunk)
        /// Support invariant that chunks must have the same number of columns as header.
        current_data.chunk = Chunk(getOutputPort().getHeader().cloneEmpty().getColumns(), 0);
}

void InflatingExpressionTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        if (expression->resultIsAlwaysEmpty())
        {
            stopReading();
            chunk = Chunk(getOutputPort().getHeader().getColumns(), 0);
            return;
        }
    }

    Block block;
    if (on_totals)
    {
        if (default_totals && !expression->hasTotalsInJoin())
            return;
        block = readExecOnTotals(chunk);
    }
    else
        block = readExec(chunk);

    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

Block InflatingExpressionTransform::readExec(Chunk & chunk)
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

Block InflatingExpressionTransform::readExecOnTotals(Chunk & chunk)
{
    Block block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    expression->executeOnTotals(block);
    return block;
}

}
