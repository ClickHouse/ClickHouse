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

CheckSortedTransform::CheckSortedTransform(
    const Block & header_,
    const SortDescription & sort_description_)
    : ISimpleTransform(header_, header_, false)
    , sort_description_map(addPositionsToSortDescriptions(sort_description_))
{
}

SortDescriptionsWithPositions
CheckSortedTransform::addPositionsToSortDescriptions(const SortDescription & sort_description)
{
    SortDescriptionsWithPositions result;
    result.reserve(sort_description.size());
    const auto & header = getInputPort().getHeader();

    for (SortColumnDescription description_copy : sort_description)
    {
        if (!description_copy.column_name.empty())
            description_copy.column_number = header.getPositionByName(description_copy.column_name);

        result.push_back(description_copy);
    }

    return result;
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

            int res = elem.direction * left_col->compareAt(left_index, right_index, *right_col, elem.nulls_direction);
            if (res < 0)
            {
                return;
            }
            else if (res > 0)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort order of blocks violated for column number {}, left: {}, right: {}.",
                    column_number,
                    applyVisitor(FieldVisitorDump(), (*left_col)[left_index]),
                    applyVisitor(FieldVisitorDump(), (*right_col)[right_index]));
            }
        }
    };

    const auto & chunk_columns = chunk.getColumns();
    if (!last_row.empty())
        check(last_row, 0, chunk_columns, 0);

    for (size_t i = 1; i < num_rows; ++i)
        check(chunk_columns, i - 1, chunk_columns, i);

    last_row.clear();
    for (const auto & chunk_column : chunk_columns)
    {
        auto column = chunk_column->cloneEmpty();
        column->insertFrom(*chunk_column, num_rows - 1);
        last_row.emplace_back(std::move(column));
    }
}

}
