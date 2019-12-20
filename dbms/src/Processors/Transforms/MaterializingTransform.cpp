#include <Processors/Transforms/MaterializingTransform.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

MaterializingTransform::MaterializingTransform(const Block & header)
    : ISimpleTransform(header, materializeBlock(header), false) {}

void MaterializingTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = col->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

}
