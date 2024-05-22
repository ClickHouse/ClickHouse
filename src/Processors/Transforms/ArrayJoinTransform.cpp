#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Interpreters/ArrayJoinAction.h>
#include "Core/Field.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block ArrayJoinTransform::transformHeader(Block header, const ArrayJoinActionPtr & array_join)
{
    auto columns = header.getColumnsWithTypeAndName();
    array_join->prepare(columns);
    Block res{std::move(columns)};
    res.setColumns(res.mutateColumns());
    return res;
}

ArrayJoinTransform::ArrayJoinTransform(
    const Block & header_,
    ArrayJoinActionPtr array_join_,
    bool /*on_totals_*/)
    : IInflatingTransform(header_, transformHeader(header_, array_join_))
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

    auto block = result_iterator->next();
    return Chunk(block.getColumns(), block.rows());
}

}
