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

Block FilterTransform::transformHeader(
    Block header,
    const ActionsDAG * expression,
    const String & filter_column_name,
    bool remove_filter_column)
{
    if (expression)
        header = expression->updateHeader(std::move(header));

    if (remove_filter_column)
        header.erase(filter_column_name);
    else
        replaceFilterToConstant(header, filter_column_name);

    return header;
}

FilterTransform::FilterTransform(
    const Block & header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_)
    : ISimpleTransform(
            header_,
            transformHeader(header_, expression_ ? &expression_->getActionsDAG() : nullptr, filter_column_name_, remove_filter_column_),
            true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
{
    transformed_header = getInputPort().getHeader();
    if (expression)
        expression->execute(transformed_header);
    filter_column_position = transformed_header.getPositionByName(filter_column_name);

    auto & column = transformed_header.getByPosition(filter_column_position).column;
    if (column)
        constant_filter_description = ConstantFilterDescription(*column);
}

IProcessor::Status FilterTransform::prepare()
{
    if (!on_totals
        && (constant_filter_description.always_false
            /// Optimization for `WHERE column in (empty set)`.
            /// The result will not change after set was created, so we can skip this check.
            /// It is implemented in prepare() stop pipeline before reading from input port.
            || (!are_prepared_sets_initialized && expression && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    auto status = ISimpleTransform::prepare();

    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    if (status != IProcessor::Status::PortFull)
        are_prepared_sets_initialized = true;

    return status;
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

    {
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        columns.clear();

        if (expression)
            expression->execute(block, num_rows_before_filtration);

        columns = block.getColumns();
    }

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

    std::unique_ptr<IFilterDescription> filter_description;
    if (filter_column->isSparse())
        filter_description = std::make_unique<SparseFilterDescription>(*filter_column);
    else
        filter_description = std::make_unique<FilterDescription>(*filter_column);

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = filter_description->filter(*columns[first_non_constant_column], -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = filter_description->countBytesInFilter();

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
        /// SimpleTransform will skip it.
        return;

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        if (!remove_filter_column)
        {
            /// Replace the column with the filter by a constant.
            auto & type = transformed_header.getByPosition(filter_column_position).type;
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
        const auto & current_type = transformed_header.safeGetByPosition(i).type;
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
            current_column = filter_description->filter(*current_column, num_filtered_rows);
    }

    chunk.setColumns(std::move(columns), num_filtered_rows);
    removeFilterIfNeed(chunk);
}


}
