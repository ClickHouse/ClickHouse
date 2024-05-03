#include <Processors/Transforms/MaterializingTransform.h>
#include <Columns/ColumnSparse.h>

#include <Common/logger_useful.h>


namespace DB
{

MaterializingTransform::MaterializingTransform(const Block & header)
    : ISimpleTransform(header, materializeBlock(header), false) {}

void MaterializingTransform::transform(Chunk & chunk)
{
    LOG_DEBUG(getLogger("MaterializingTransform"),
              "transform {}", chunk.getNumRows());

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = recursiveRemoveSparse(col->convertToFullColumnIfConst());

    chunk.setColumns(std::move(columns), num_rows);
}

}
