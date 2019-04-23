#include "FillingBlockInputStream.h"

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_WITH_FILL_EXPRESSION;
}

namespace detail
{

ColumnRawPtrs getColumnsExcept(SharedBlockPtr & block_ptr, ColumnRawPtrs & except_columns)
{
    ColumnRawPtrs res;
    res.reserve(block_ptr->columns() - except_columns.size());

    for (size_t i = 0; i < block_ptr->columns(); ++i)
    {
        const IColumn * raw_col = block_ptr->safeGetByPosition(i).column.get();
        if (std::find(except_columns.begin(), except_columns.end(), raw_col) == except_columns.end())
            res.emplace_back(raw_col);
    }

    return res;
}

void copyRowFromColumns(ColumnRawPtrs & from, ColumnRawPtrs & to, size_t row_num)
{
    for (size_t i = 0; i < from.size(); ++i)
        const_cast<IColumn *>(to[i])->insertFrom(*from[i], row_num);
}

void fillRestOfRow(
        size_t cols_copied, ColumnRawPtrs & res_fill_columns, ColumnRawPtrs & res_rest_columns,
        ColumnRawPtrs & old_rest_columns, UInt64 & next_row_num)
{
    /// step_val was inserted, fill all other columns with default values
    if (cols_copied < res_fill_columns.size())
    {
        for (; cols_copied < res_fill_columns.size(); ++cols_copied)
            const_cast<IColumn *>(res_fill_columns[cols_copied])->insertDefault();
        for (size_t it = 0; it < res_rest_columns.size(); ++it)
            const_cast<IColumn *>(res_rest_columns[it])->insertDefault();

        return;
    }

    /// fill row wasn't created, copy rest values from row
    detail::copyRowFromColumns(old_rest_columns, res_rest_columns, next_row_num);
    ++next_row_num;
}

Field sumTwoFields(Field & a, Field & b)
{
    switch (a.getType())
    {
        case Field::Types::Null:        return a;
        case Field::Types::UInt64:      return a.get<UInt64>()  + b.get<UInt64>();
        case Field::Types::Int64:       return a.get<Int64>()   + b.get<Int64>();
        case Field::Types::Int128:      return a.get<Int128>()  + b.get<Int128>();
        case Field::Types::Float64:     return a.get<Float64>() + b.get<Float64>();
        default:
            throw Exception("WITH FILL can be used only with numeric types", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
    }
}

}

FillingBlockInputStream::FillingBlockInputStream(
        const BlockInputStreamPtr & input, const SortDescription & fill_description_)
        : fill_description(fill_description_)
{
    children.push_back(input);
}


Block FillingBlockInputStream::readImpl()
{
    Block old_block;
    UInt64 rows = 0;

    old_block = children.back()->read();
    if (!old_block)
        return old_block;

    Block res_block = old_block.cloneEmpty();

    rows = old_block.rows();

    SharedBlockPtr old_block_ptr = new detail::SharedBlock(std::move(old_block));
    ColumnRawPtrs old_fill_columns = SharedBlockRowRef::getBlockColumns(*old_block_ptr, fill_description);
    ColumnRawPtrs old_rest_columns = detail::getColumnsExcept(old_block_ptr, old_fill_columns);

    SharedBlockPtr res_block_ptr = new detail::SharedBlock(std::move(res_block));
    ColumnRawPtrs res_fill_columns = SharedBlockRowRef::getBlockColumns(*res_block_ptr, fill_description);
    ColumnRawPtrs res_rest_columns = detail::getColumnsExcept(res_block_ptr, res_fill_columns);

    /// number of next row in current block
    UInt64 next_row_num = 0;

    /// read first block
    if (!pos)
    {
        ++next_row_num;
        /// create row number 0 in result block here
        detail::copyRowFromColumns(old_fill_columns, res_fill_columns, 0);
        detail::copyRowFromColumns(old_rest_columns, res_rest_columns, 0);
    }

    /// current block is not first, need to compare with row in other block
    if (!next_row_num)
    {
        size_t cnt_cols = 0;
        size_t fill_columns_size = old_fill_columns.size();
        for (; cnt_cols < fill_columns_size; ++cnt_cols)
        {
            Field step = fill_description[cnt_cols].fill_description.fill_step;
            Field next_val;
            Field prev_val;
            old_fill_columns[cnt_cols]->get(next_row_num, next_val);
            (*last_row_ref.columns)[cnt_cols]->get(last_row_ref.row_num, prev_val);
            Field step_val = detail::sumTwoFields(prev_val, step);
            if (step_val >= next_val)
                const_cast<IColumn *>(res_fill_columns[cnt_cols])->insertFrom(
                        *old_fill_columns[cnt_cols], next_row_num);
            else
            {
                const_cast<IColumn *>(res_fill_columns[cnt_cols])->insert(step_val);
                break;
            }
        }
        /// create row number 0 in result block here
        detail::fillRestOfRow(cnt_cols, res_fill_columns, res_rest_columns, old_rest_columns, next_row_num);
        ++pos;
    }

    /// number of last added row in result block
    UInt64 last_row_num = 0;

    while (next_row_num < rows)
    {
        size_t cnt_cols = 0;
        size_t fill_columns_size = old_fill_columns.size();
        for (; cnt_cols < fill_columns_size; ++cnt_cols)
        {
            Field step = fill_description[cnt_cols].fill_description.fill_step;
            Field prev_val;
            res_fill_columns[cnt_cols]->get(last_row_num, prev_val);
            Field step_val = detail::sumTwoFields(prev_val, step);
            Field next_val;
            old_fill_columns[cnt_cols]->get(next_row_num, next_val);
            if (step_val >= next_val)
                const_cast<IColumn *>(res_fill_columns[cnt_cols])->insertFrom(
                        *old_fill_columns[cnt_cols], next_row_num);
            else
            {
                const_cast<IColumn *>(res_fill_columns[cnt_cols])->insert(step_val);
                break;
            }
        }

        /// create new row in result block, increment last_row_num
        detail::fillRestOfRow(cnt_cols, res_fill_columns, res_rest_columns, old_rest_columns, next_row_num);
        ++last_row_num;
        ++pos;
    }

    /// finished current block, need to remember last row
    SharedBlockRowRef::setSharedBlockRowRef(last_row_ref, res_block_ptr, & res_fill_columns, last_row_num);
    return *res_block_ptr;
}


}