#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <DataStreams/finalizeBlock.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/Arena.h>


namespace DB
{


TotalsHavingBlockInputStream::TotalsHavingBlockInputStream(
    const BlockInputStreamPtr & input_,
    bool overflow_row_, const ExpressionActionsPtr & expression_,
    const std::string & filter_column_, TotalsMode totals_mode_, double auto_include_threshold_, bool final_)
    : overflow_row(overflow_row_),
    expression(expression_), filter_column_name(filter_column_), totals_mode(totals_mode_),
    auto_include_threshold(auto_include_threshold_), final(final_)
{
    children.push_back(input_);

    /// Initialize current totals with initial state.

    Block source_header = children.at(0)->getHeader();

    current_totals.reserve(source_header.columns());
    for (const auto & elem : source_header)
    {
        // Create a column with default value
        MutableColumnPtr new_column = elem.type->createColumn();
        elem.type->insertDefaultInto(*new_column);
        current_totals.emplace_back(std::move(new_column));
    }
}


Block TotalsHavingBlockInputStream::getTotals()
{
    if (!totals)
    {
        /** If totals_mode == AFTER_HAVING_AUTO, you need to decide whether to add aggregates to TOTALS for strings,
          *  not passed max_rows_to_group_by.
          */
        if (overflow_aggregates)
        {
            if (totals_mode == TotalsMode::BEFORE_HAVING
                || totals_mode == TotalsMode::AFTER_HAVING_INCLUSIVE
                || (totals_mode == TotalsMode::AFTER_HAVING_AUTO
                    && static_cast<double>(passed_keys) / total_keys >= auto_include_threshold))
                addToTotals(overflow_aggregates, nullptr);
        }

        totals = children.at(0)->getHeader().cloneWithColumns(std::move(current_totals));
        finalizeBlock(totals);
    }

    if (totals && expression)
        expression->execute(totals);

    return totals;
}


Block TotalsHavingBlockInputStream::getHeader() const
{
    Block res = children.at(0)->getHeader();
    if (final)
        finalizeBlock(res);
    if (expression)
        expression->execute(res);
    return res;
}


Block TotalsHavingBlockInputStream::readImpl()
{
    Block finalized;
    Block block;

    while (true)
    {
        block = children[0]->read();

        /// Block with values not included in `max_rows_to_group_by`. We'll postpone it.
        if (overflow_row && block && block.info.is_overflows)
        {
            overflow_aggregates = block;
            continue;
        }

        if (!block)
            return finalized;

        finalized = block;
        if (final)
            finalizeBlock(finalized);

        total_keys += finalized.rows();

        if (filter_column_name.empty())
        {
            addToTotals(block, nullptr);
        }
        else
        {
            /// Compute the expression in HAVING.
            expression->execute(finalized);

            size_t filter_column_pos = finalized.getPositionByName(filter_column_name);
            ColumnPtr filter_column_ptr = finalized.safeGetByPosition(filter_column_pos).column->convertToFullColumnIfConst();

            FilterDescription filter_description(*filter_column_ptr);

            /// Add values to `totals` (if it was not already done).
            if (totals_mode == TotalsMode::BEFORE_HAVING)
                addToTotals(block, nullptr);
            else
                addToTotals(block, filter_description.data);

            /// Filter the block by expression in HAVING.
            size_t columns = finalized.columns();

            for (size_t i = 0; i < columns; ++i)
            {
                ColumnWithTypeAndName & current_column = finalized.safeGetByPosition(i);
                current_column.column = current_column.column->filter(*filter_description.data, -1);
                if (current_column.column->empty())
                {
                    finalized.clear();
                    break;
                }
            }
        }

        if (!finalized)
            continue;

        passed_keys += finalized.rows();
        return finalized;
    }
}


void TotalsHavingBlockInputStream::addToTotals(const Block & source_block, const IColumn::Filter * filter)
{
    for (size_t i = 0, num_columns = source_block.columns(); i < num_columns; ++i)
    {
        const auto * source_column = typeid_cast<const ColumnAggregateFunction *>(
                    source_block.getByPosition(i).column.get());
        if (!source_column)
        {
            continue;
        }

        auto & totals_column = assert_cast<ColumnAggregateFunction &>(*current_totals[i]);
        assert(totals_column.size() == 1);

        /// Accumulate all aggregate states from a column of a source block into
        /// the corresponding totals column.
        const auto & vec = source_column->getData();
        size_t size = vec.size();

        if (filter)
        {
            for (size_t j = 0; j < size; ++j)
                if ((*filter)[j])
                    totals_column.insertMergeFrom(vec[j]);
        }
        else
        {
            for (size_t j = 0; j < size; ++j)
                totals_column.insertMergeFrom(vec[j]);
        }
    }
}

}
