#include <Processors/Transforms/RemovingSparseTransform.h>
#include <Columns/ColumnSparse.h>


namespace DB
{

RemovingSparseTransform::RemovingSparseTransform(SharedHeader header)
    : ISimpleTransform(header, std::make_shared<const Block>(materializeBlock(*header)), false)
{
}

void RemovingSparseTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = recursiveRemoveSparse(col);

    chunk.setColumns(std::move(columns), num_rows);
}

}
