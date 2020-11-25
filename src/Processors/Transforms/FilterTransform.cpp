#include <Processors/Transforms/FilterTransform.h>

#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>

namespace DB
{

static void replaceFilterToConstant(Block & block, const String & filter_column_name)
{
    ConstantFilterDescription constant_filter_description;

    auto filter_column = block.getPositionByName(filter_column_name);
    auto & column_elem = block.safeGetByPosition(filter_column);

    /// Isn't the filter already constant?
    if (column_elem.column)
        constant_filter_description = ConstantFilterDescription(*column_elem.column);

    if (!constant_filter_description.always_false
        && !constant_filter_description.always_true)
    {
        /// Replace the filter column to a constant with value 1.
        FilterDescription filter_description_check(*column_elem.column);
        column_elem.column = column_elem.type->createColumnConst(block.rows(), 1u);
    }
}

Block FilterTransform::transformHeader(Block header, const String & filter_column_name, bool remove_filter_column)
{
    if (remove_filter_column)
        header.erase(filter_column_name);
    else
        replaceFilterToConstant(header, filter_column_name);

    return header;
}

FilterTransform::FilterTransform(
    const Block & header_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_)
    : ISimpleTransform(header_, transformHeader(header_, filter_column_name_, remove_filter_column_), true)
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
{
    const auto & input_header = getInputPort().getHeader();
    filter_column_position = input_header.getPositionByName(filter_column_name);

    const auto & column = input_header.getByPosition(filter_column_position).column;
    if (column)
        constant_filter_description = ConstantFilterDescription(*column);
}

IProcessor::Status FilterTransform::prepare()
{
    if (!on_totals && constant_filter_description.always_false)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    return ISimpleTransform::prepare();
}


void FilterTransform::removeFilterIfNeed(Chunk & chunk) const
{
    if (chunk && remove_filter_column)
        chunk.erase(filter_column_position);
}

void FilterTransform::transform(Chunk & chunk)
{
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    if (constant_filter_description.always_true || on_totals)
    {
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];

    /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
        *  and now - are calculated. That is, not all cases are covered by the code above.
        * This happens if the function returns a constant for a non-constant argument.
        * For example, `ignore` function.
        */
    constant_filter_description = ConstantFilterDescription(*filter_column);

    if (constant_filter_description.always_false)
        return; /// Will finish at next prepare call

    if (constant_filter_description.always_true)
    {
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    FilterDescription filter_and_holder(*filter_column);

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != filter_column_position && !isColumnConst(*columns[i]))
        {
            first_non_constant_column = i;
            break;
        }
    }

    size_t num_filtered_rows;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = columns[first_non_constant_column]->filter(*filter_and_holder.data, -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = countBytesInFilter(*filter_and_holder.data);

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
        /// SimpleTransform will skip it.
        return;

    const auto & input_header = getInputPort().getHeader();

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        if (!remove_filter_column)
        {
            /// Replace the column with the filter by a constant.
            const auto & type = input_header.getByPosition(filter_column_position).type;
            columns[filter_column_position] = type->createColumnConst(num_filtered_rows, 1u);
        }

        /// No need to touch the rest of the columns.
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & current_type = input_header.safeGetByPosition(i).type;
        auto & current_column = columns[i];

        if (i == filter_column_position)
        {
            /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
            /// NOTE User could pass column with something different than 0 and 1 for filter.
            /// Example:
            ///  SELECT materialize(100) AS x WHERE x
            /// will work incorrectly.
            current_column = current_type->createColumnConst(num_filtered_rows, 1u);
            continue;
        }

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = current_column->filter(*filter_and_holder.data, num_filtered_rows);
    }

    chunk.setColumns(std::move(columns), num_filtered_rows);
    removeFilterIfNeed(chunk);
}


}
