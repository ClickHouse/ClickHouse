#include <Processors/Transforms/CheckSortedTransform.h>
#include <Common/FieldVisitorDump.h>
#include <Common/quoteString.h>
#include <Core/SortDescription.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CheckSortedTransform::CheckSortedTransform(const Block & header, const SortDescription & sort_description)
    : ISimpleTransform(header, header, false)
{
    for (const auto & column_description : sort_description)
        sort_description_map.emplace_back(column_description, header.getPositionByName(column_description.column_name));
}

void CheckSortedTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto check = [this](const Columns & left, size_t left_index, const Columns & right, size_t right_index)
    {
        for (const auto & elem : sort_description_map)
        {
            size_t column_number = elem.column_number;

            const IColumn * left_col = left[column_number].get();
            const IColumn * right_col = right[column_number].get();

            int res = elem.base.direction * left_col->compareAt(left_index, right_index, *right_col, elem.base.nulls_direction);
            if (res < 0)
            {
                return;
            }
            if (res > 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sort order of blocks violated for column number {}, left: {}, right: {}. Chunk {}, rows read {}.{}",
                    column_number,
                    applyVisitor(FieldVisitorDump(), (*left_col)[left_index]),
                    applyVisitor(FieldVisitorDump(), (*right_col)[right_index]),
                    chunk_num,
                    rows_read,
                    description.empty() ? String() : fmt::format(" ({})", description));
            }
        }
    };

    /// ColumnVector tries to cast the rhs column to the same type (ColumnVector) in compareAt method.
    /// And it doesn't care about the possible incompatibilities in data types
    /// (for example in case when the right column is ColumnSparse)
    convertToFullIfSparse(chunk);

    const auto & chunk_columns = chunk.getColumns();

    ++rows_read;

    if (!last_row.empty())
        check(last_row, 0, chunk_columns, 0);

    for (size_t i = 1; i < num_rows; ++i)
    {
        ++rows_read;
        check(chunk_columns, i - 1, chunk_columns, i);
    }

    last_row.clear();
    for (const auto & chunk_column : chunk_columns)
    {
        auto column = chunk_column->cloneEmpty();
        column->insertFrom(*chunk_column, num_rows - 1);
        last_row.emplace_back(std::move(column));
    }

    ++chunk_num;
}

}
