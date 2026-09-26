#include <Processors/Transforms/RemovingReplicatedColumnsTransform.h>
#include <Columns/ColumnSparse.h>


namespace DB
{

RemovingReplicatedColumnsTransform::RemovingReplicatedColumnsTransform(SharedHeader header)
    : ISimpleTransform(header, std::make_shared<const Block>(materializeBlock(*header)), false)
{
}

void RemovingReplicatedColumnsTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = col->convertToFullColumnIfReplicated();

    chunk.setColumns(std::move(columns), num_rows);
}

}
