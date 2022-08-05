#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & expression)
{
    return expression.updateHeader(std::move(header));
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG()), false)
    , expression(std::move(expression_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    std::cerr << "[ExpressionTransform] ===" << std::endl;
    std::cerr << "[ExpressionTransform] before transform got chunk with structure: " << chunk.dumpStructure() << std::endl;
    std::cerr << "[ExpressionTransform] input port structure " << getInputPort().getHeader().dumpStructure() << std::endl;
    std::cerr << "[ExpressionTransform] output port structure " << getInputPort().getHeader().dumpStructure() << std::endl;

    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    std::cerr << "[ExpressionTransform] executing actions: " << expression->dumpActions() << std::endl;

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);

    std::cerr << "[ExpressionTransform] after transform got chunk with structure: " << chunk.dumpStructure() << std::endl;
    std::cerr << "[ExpressionTransform] ===" << std::endl;
}

ConvertingTransform::ConvertingTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, ExpressionTransform::transformHeader(header_, expression_->getActionsDAG()))
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
