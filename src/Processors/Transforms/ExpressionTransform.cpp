#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    expression->execute(header, true);
    return header;
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, transformHeader(header_, expression_), false)
    , expression(std::move(expression_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    if (!block)
        block.insert({DataTypeUInt8().createColumnConst(num_rows, 0), std::make_shared<DataTypeUInt8>(), "_dummy"});

    expression->execute(block);

    num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

}
