#include <Processors/Transforms/PartialSortingTransform.h>
#include <Interpreters/sortBlock.h>

namespace DB
{

PartialSortingTransform::PartialSortingTransform(const Block & header, SortDescription & description, UInt64 limit)
    : ISimpleTransform(header, header, false)
    , description(description), limit(limit)
{
}

void PartialSortingTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    read_rows += num_rows;

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    chunk.clear();

    sortBlock(block, description, limit);
    chunk.setColumns(block.getColumns(), num_rows);
}

}
