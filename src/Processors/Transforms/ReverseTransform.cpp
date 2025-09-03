#include <Columns/IColumn.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Common/PODArray.h>

namespace DB
{

void ReverseTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Permutation permutation(num_rows);

    for (size_t i = 0; i < num_rows; ++i)
        permutation[i] = num_rows - 1 - i;

    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->permute(permutation, 0);

    chunk.setColumns(std::move(columns), num_rows);
}

}
