#include <Processors/Transforms/RemovingSparseTransform.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>


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

    /// The output header is a materialized block, so non-native LowCardinality columns (automatic
    /// LowCardinality serialization) have to be materialized as well to match it.
    for (auto & col : columns)
        col = recursiveRemoveNonNativeLowCardinality(recursiveRemoveSparse(col));

    chunk.setColumns(std::move(columns), num_rows);
}

}
