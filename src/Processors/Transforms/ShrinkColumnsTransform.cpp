#include <Processors/Transforms/ShrinkColumnsTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Chunk.h>

namespace DB
{

namespace
{

/// Shrinks a column to fit (reclaiming reserved-but-unused memory, e.g. from power-of-two growth of
/// variable-length columns) when it is worth it:
///   - the column is uniquely owned (`use_count() == 1`), so shrinking does not clone a shared column
///     (which would transiently increase memory instead of reducing it);
///   - the reserved memory noticeably exceeds the used memory:
///     `allocatedBytes() > byteSize() * min_waste_ratio` and `allocatedBytes() - byteSize() >= min_waste_bytes`.
void shrinkColumnToFitIfBeneficial(ColumnPtr & column, double min_waste_ratio, size_t min_waste_bytes)
{
    if (!column || column->use_count() != 1)
        return;

    const size_t used = column->byteSize();
    const size_t allocated = column->allocatedBytes();
    if (allocated <= used || allocated - used < min_waste_bytes)
        return;
    if (static_cast<double>(allocated) <= static_cast<double>(used) * min_waste_ratio)
        return;

    auto mutable_column = IColumn::mutate(std::move(column));
    mutable_column->shrinkToFit();
    column = std::move(mutable_column);
}

}

ShrinkColumnsTransform::ShrinkColumnsTransform(SharedHeader header, double min_waste_ratio_, size_t min_waste_bytes_)
    : ISimpleTransform(header, header, false)
    , min_waste_ratio(min_waste_ratio_)
    , min_waste_bytes(min_waste_bytes_)
{
}

void ShrinkColumnsTransform::transform(Chunk & chunk)
{
    if (min_waste_ratio <= 1.0 || !chunk.hasColumns())
        return;

    const size_t num_rows = chunk.getNumRows();
    Columns columns = chunk.detachColumns();
    for (auto & column : columns)
        shrinkColumnToFitIfBeneficial(column, min_waste_ratio, min_waste_bytes);
    chunk.setColumns(std::move(columns), num_rows);
}

}
