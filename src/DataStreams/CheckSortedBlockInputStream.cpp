#include <DataStreams/CheckSortedBlockInputStream.h>
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

/// Compares values in columns. Columns must have equal types.
struct SortingLessOrEqualComparator
{
    const SortDescriptionsWithPositions & sort_description;

    explicit SortingLessOrEqualComparator(const SortDescriptionsWithPositions & sort_description_)
        : sort_description(sort_description_) {}

    bool operator()(const Columns & left, size_t left_index, const Columns & right, size_t right_index) const
    {
        for (const auto & elem : sort_description)
        {
            size_t column_number = elem.column_number;

            const IColumn * left_col = left[column_number].get();
            const IColumn * right_col = right[column_number].get();

            int res = elem.direction * left_col->compareAt(left_index, right_index, *right_col, elem.nulls_direction);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return true;
    }
};

Block CheckSortedBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (!block || block.rows() == 0)
        return block;

    SortingLessOrEqualComparator less(sort_description_map);

    auto block_columns = block.getColumns();
    if (!last_row.empty() && !less(last_row, 0, block_columns, 0))
        throw Exception("Sort order of blocks violated", ErrorCodes::LOGICAL_ERROR);

    size_t rows = block.rows();
    for (size_t i = 1; i < rows; ++i)
        if (!less(block_columns, i - 1, block_columns, i))
            throw Exception("Sort order of blocks violated", ErrorCodes::LOGICAL_ERROR);

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
