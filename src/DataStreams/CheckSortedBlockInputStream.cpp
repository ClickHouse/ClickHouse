#include <DataStreams/CheckSortedBlockInputStream.h>
#include <Common/FieldVisitorDump.h>
#include <Common/quoteString.h>
#include <Core/SortDescription.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CheckSortedBlockInputStream::CheckSortedBlockInputStream(
    const BlockInputStreamPtr & input_,
    const SortDescription & sort_description_)
    : header(input_->getHeader())
    , sort_description_map(addPositionsToSortDescriptions(sort_description_))
{
    children.push_back(input_);
}

SortDescriptionsWithPositions
CheckSortedBlockInputStream::addPositionsToSortDescriptions(const SortDescription & sort_description)
{
    SortDescriptionsWithPositions result;
    result.reserve(sort_description.size());

    for (SortColumnDescription description_copy : sort_description)
    {
        if (!description_copy.column_name.empty())
            description_copy.column_number = header.getPositionByName(description_copy.column_name);

        result.push_back(description_copy);
    }

    return result;
}


Block CheckSortedBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (!block || block.rows() == 0)
        return block;

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

    auto block_columns = block.getColumns();
    if (!last_row.empty())
        check(last_row, 0, block_columns, 0);

    size_t rows = block.rows();
    for (size_t i = 1; i < rows; ++i)
        check(block_columns, i - 1, block_columns, i);

    last_row.clear();
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto column = block_columns[i]->cloneEmpty();
        column->insertFrom(*block_columns[i], rows - 1);
        last_row.emplace_back(std::move(column));
    }

    return block;
}

}
