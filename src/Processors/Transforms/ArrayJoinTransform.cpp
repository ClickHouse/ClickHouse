#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Interpreters/ArrayJoinAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block ArrayJoinTransform::transformHeader(Block header, const ArrayJoinActionPtr & array_join)
{
    array_join->execute(header);
    return header;
}

ArrayJoinTransform::ArrayJoinTransform(
    const Block & header_,
    ArrayJoinActionPtr array_join_,
    bool /*on_totals_*/)
    : ISimpleTransform(header_, transformHeader(header_, array_join_), false)
    , array_join(std::move(array_join_))
{
    /// TODO
//    if (on_totals_)
//        throw Exception("ARRAY JOIN is not supported for totals", ErrorCodes::LOGICAL_ERROR);
}

void ArrayJoinTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    array_join->execute(block);
    chunk.setColumns(block.getColumns(), block.rows());
}

}
