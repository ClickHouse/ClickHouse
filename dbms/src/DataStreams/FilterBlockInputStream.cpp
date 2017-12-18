#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/typeid_cast.h>

#include <DataStreams/FilterBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


FilterBlockInputStream::FilterBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, ssize_t filter_column_)
    : expression(expression_), filter_column(filter_column_)
{
    children.push_back(input);
}

FilterBlockInputStream::FilterBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, const String & filter_column_name_)
    : expression(expression_), filter_column(-1), filter_column_name(filter_column_name_)
{
    children.push_back(input);
}


String FilterBlockInputStream::getName() const { return "Filter"; }


String FilterBlockInputStream::getID() const
{
    std::stringstream res;
    res << "Filter(" << children.back()->getID() << ", " << expression->getID() << ", " << filter_column << ", " << filter_column_name << ")";
    return res.str();
}


const Block & FilterBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
}


Block FilterBlockInputStream::readImpl()
{
    Block res;

    if (is_first)
    {
        is_first = false;

        const Block & sample_block = expression->getSampleBlock();

        /// Find the current position of the filter column in the block.
        /** sample_block has the result structure of evaluating the expression.
          * But this structure does not necessarily match expression->execute(res) below,
          *  because the expression can be applied to a block that also contains additional,
          *  columns unnecessary for this expression, but needed later, in the next stages of the query execution pipeline.
          * There will be no such columns in sample_block.
          * Therefore, the position of the filter column in it can be different.
          */
        ssize_t filter_column_in_sample_block = filter_column;
        if (filter_column_in_sample_block == -1)
            filter_column_in_sample_block = sample_block.getPositionByName(filter_column_name);

        /// Let's check if the filter column is a constant containing 0 or 1.
        ColumnPtr column = sample_block.safeGetByPosition(filter_column_in_sample_block).column;

        if (column)
            constant_filter_description = ConstantFilterDescription(*column);

        if (constant_filter_description.always_false)
            return res;
    }

    /// Until non-empty block after filtering or end of stream.
    while (1)
    {
        res = children.back()->read();
        if (!res)
            return res;

        expression->execute(res);

        if (constant_filter_description.always_true)
            return res;

        /// Find the current position of the filter column in the block.
        if (filter_column == -1)
            filter_column = res.getPositionByName(filter_column_name);

        size_t columns = res.columns();
        ColumnPtr column = res.safeGetByPosition(filter_column).column;

        /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
            *  and now - are calculated. That is, not all cases are covered by the code above.
            * This happens if the function returns a constant for a non-constant argument.
            * For example, `ignore` function.
            */
        constant_filter_description = ConstantFilterDescription(*column);

        if (constant_filter_description.always_false)
        {
            res.clear();
            return res;
        }

        if (constant_filter_description.always_true)
            return res;

        FilterDescription filter_and_holder(*column);

        /** Let's find out how many rows will be in result.
          * To do this, we filter out the first non-constant column
          *  or calculate number of set bytes in the filter.
          */
        size_t first_non_constant_column = 0;
        for (size_t i = 0; i < columns; ++i)
        {
            if (!res.safeGetByPosition(i).column->isColumnConst())
            {
                first_non_constant_column = i;

                if (first_non_constant_column != static_cast<size_t>(filter_column))
                    break;
            }
        }

        size_t filtered_rows = 0;
        if (first_non_constant_column != static_cast<size_t>(filter_column))
        {
            ColumnWithTypeAndName & current_column = res.safeGetByPosition(first_non_constant_column);
            current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
            filtered_rows = current_column.column->size();
        }
        else
        {
            filtered_rows = countBytesInFilter(*filter_and_holder.data);
        }

        /// If the current block is completely filtered out, let's move on to the next one.
        if (filtered_rows == 0)
            continue;

        /// If all the rows pass through the filter.
        if (filtered_rows == filter_and_holder.data->size())
        {
            /// Replace the column with the filter by a constant.
            res.safeGetByPosition(filter_column).column = res.safeGetByPosition(filter_column).type->createColumnConst(filtered_rows, UInt64(1));
            /// No need to touch the rest of the columns.
            return res;
        }

        /// Filter the rest of the columns.
        for (size_t i = 0; i < columns; ++i)
        {
            ColumnWithTypeAndName & current_column = res.safeGetByPosition(i);

            if (i == static_cast<size_t>(filter_column))
            {
                /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
                /// NOTE User could pass column with something different than 0 and 1 for filter.
                /// Example:
                ///  SELECT materialize(100) AS x WHERE x
                /// will work incorrectly.
                current_column.column = current_column.type->createColumnConst(filtered_rows, UInt64(1));
                continue;
            }

            if (i == first_non_constant_column)
                continue;

            if (current_column.column->isColumnConst())
                current_column.column = current_column.column->cut(0, filtered_rows);
            else
                current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
        }

        return res;
    }
}


}
