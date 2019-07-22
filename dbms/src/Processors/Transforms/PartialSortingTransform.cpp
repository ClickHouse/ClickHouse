#include <Processors/Transforms/PartialSortingTransform.h>
#include <Interpreters/sortBlock.h>

namespace DB
{

PartialSortingTransform::PartialSortingTransform(
    const Block & header, SortDescription & description, UInt64 limit, bool do_count_rows)
    : ISimpleTransform(header, header, false)
    , description(description), limit(limit), do_count_rows(do_count_rows)
{
}

void PartialSortingTransform::transform(Chunk & chunk)
{
    if (do_count_rows)
        read_rows += chunk.getNumRows();

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    chunk.clear();

    sortBlock(block, description, limit);
    chunk.setColumns(block.getColumns(), block.rows());
}

}
