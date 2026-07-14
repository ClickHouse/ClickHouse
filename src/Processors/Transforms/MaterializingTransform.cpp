#include <Processors/Transforms/MaterializingTransform.h>
#include <Columns/ColumnSparse.h>


namespace DB
{

MaterializingTransform::MaterializingTransform(SharedHeader header, bool remove_sparse_)
    : ISimpleTransform(header, std::make_shared<const Block>(materializeBlock(*header)), false)
    , remove_sparse(remove_sparse_)
{
}

void MaterializingTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
    {
        col = col->convertToFullColumnIfConst();
        if (remove_sparse)
            col = recursiveRemoveSparse(col);
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
