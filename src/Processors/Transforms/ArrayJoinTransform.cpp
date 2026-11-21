#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <Functions/IFunction.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Container>
Block transformHeaderImpl(const Block & header, const Container & array_join_columns)
{
    auto columns = header.getColumnsWithTypeAndName();
    ArrayJoinAction::prepare(array_join_columns, columns);
    Block res{std::move(columns)};
    res.setColumns(res.mutateColumns());
    return res;
}

Block ArrayJoinTransform::transformHeader(const Block & header, const Names & array_join_columns)
{
    return transformHeaderImpl(header, array_join_columns);
}

ArrayJoinTransform::ArrayJoinTransform(
    SharedHeader header_,
    ArrayJoinActionPtr array_join_,
    bool /*on_totals_*/)
    : IInflatingTransform(header_, std::make_shared<const Block>(transformHeaderImpl(*header_, array_join_->columns)))
    , array_join(std::move(array_join_))
{
    /// TODO
//    if (on_totals_)
//        throw Exception(ErrorCodes::LOGICAL_ERROR, "ARRAY JOIN is not supported for totals");
}

void ArrayJoinTransform::consume(Chunk chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    result_iterator = array_join->execute(block);
}


bool ArrayJoinTransform::canGenerate()
{
    return result_iterator && result_iterator->hasNext();
}

Chunk ArrayJoinTransform::generate()
{
    if (!canGenerate())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in ArrayJoinTransform");

    auto block = result_iterator->next(&getCancellationFlag());
    if (isCancelled())
        return Chunk(getOutputPort().getHeader().cloneEmptyColumns(), 0);
    return Chunk(block.getColumns(), block.rows());
}

void ArrayJoinTransform::onCancel() noexcept
{
    IInflatingTransform::onCancel();
    if (!array_join->element_filter)
        return;
    for (const auto & node : array_join->element_filter->getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
            node.function->cancelExecution();
    }
}

}
