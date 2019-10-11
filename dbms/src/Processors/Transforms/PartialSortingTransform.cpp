#include <Processors/Transforms/PartialSortingTransform.h>
#include <Interpreters/sortBlock.h>

namespace DB
{

PartialSortingTransform::PartialSortingTransform(
    const Block & header_, SortDescription & description_, UInt64 limit_, bool do_count_rows_)
    : ISimpleTransform(header_, header_, false)
    , description(description_), limit(limit_), do_count_rows(do_count_rows_)
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
