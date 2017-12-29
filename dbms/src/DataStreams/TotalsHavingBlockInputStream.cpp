#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/AggregateDescription.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


TotalsHavingBlockInputStream::TotalsHavingBlockInputStream(
    const BlockInputStreamPtr & input_,
    bool overflow_row_, const ExpressionActionsPtr & expression_,
    const std::string & filter_column_, TotalsMode totals_mode_, double auto_include_threshold_)
    : overflow_row(overflow_row_),
    expression(expression_), filter_column_name(filter_column_), totals_mode(totals_mode_),
    auto_include_threshold(auto_include_threshold_)
{
    children.push_back(input_);
}


String TotalsHavingBlockInputStream::getID() const
{
    std::stringstream res;
    res << "TotalsHavingBlockInputStream(" << children.back()->getID()
        << "," << filter_column_name << ")";
    return res.str();
}


static void finalize(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnWithTypeAndName & current = block.safeGetByPosition(i);
        const ColumnAggregateFunction * unfinalized_column = typeid_cast<const ColumnAggregateFunction *>(current.column.get());

        if (unfinalized_column)
        {
            current.type = unfinalized_column->getAggregateFunction()->getReturnType();
            current.column = unfinalized_column->convertToValues();
        }
    }
}


const Block & TotalsHavingBlockInputStream::getTotals()
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
                addToTotals(current_totals, overflow_aggregates, nullptr);
        }

        totals = header.cloneWithColumns(std::move(current_totals));
        finalize(totals);
    }

    if (totals && expression)
        expression->execute(totals);

    return totals;
}


Block TotalsHavingBlockInputStream::readImpl()
{
    Block finalized;
    Block block;

    while (1)
    {
        block = children[0]->read();

        if (!header)
            header = block.cloneEmpty();

        /// Block with values not included in `max_rows_to_group_by`. We'll postpone it.
        if (overflow_row && block && block.info.is_overflows)
        {
            overflow_aggregates = block;
            continue;
        }

        if (!block)
            return finalized;

        finalized = block;
        finalize(finalized);

        total_keys += finalized.rows();

        if (filter_column_name.empty())
        {
            addToTotals(current_totals, block, nullptr);
        }
        else
        {
            /// Compute the expression in HAVING.
            expression->execute(finalized);

            size_t filter_column_pos = finalized.getPositionByName(filter_column_name);
            ColumnPtr filter_column_ptr = finalized.safeGetByPosition(filter_column_pos).column;

            if (ColumnPtr materialized = filter_column_ptr->convertToFullColumnIfConst())
                filter_column_ptr = materialized;

            FilterDescription filter_description(*filter_column_ptr);

            /// Add values to `totals` (if it was not already done).
            if (totals_mode == TotalsMode::BEFORE_HAVING)
                addToTotals(current_totals, block, nullptr);
            else
                addToTotals(current_totals, block, filter_description.data);

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


void TotalsHavingBlockInputStream::addToTotals(MutableColumns & totals, const Block & block, const IColumn::Filter * filter)
{
    bool need_init = totals.empty();

    ArenaPtr arena;
    if (need_init)
        arena = std::make_shared<Arena>();

    for (size_t i = 0, num_columns = block.columns(); i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & current = block.safeGetByPosition(i);
        const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(current.column.get());

        if (!column)
        {
            if (need_init)
            {
                MutableColumnPtr new_column = current.type->createColumn();
                current.type->insertDefaultInto(*new_column);
                totals.emplace_back(std::move(new_column));
            }
            continue;
        }

        IAggregateFunction * function;
        AggregateDataPtr data;

        /// Create ColumnAggregateFunction with one value.

        if (need_init)
        {
            function = column->getAggregateFunction().get();
            auto target = ColumnAggregateFunction::create(column->getAggregateFunction(), Arenas(1, arena));

            data = arena->alloc(function->sizeOfData());
            function->create(data);
            target->getData().push_back(data);

            totals.emplace_back(std::move(target));
        }
        else
        {
            auto & target = typeid_cast<ColumnAggregateFunction &>(*totals[i]);
            function = target.getAggregateFunction().get();
            data = target.getData()[0];
        }

        /// Accumulate all aggregate states into that value.

        const ColumnAggregateFunction::Container & vec = column->getData();
        size_t size = vec.size();

        if (filter)
        {
            for (size_t j = 0; j < size; ++j)
                if ((*filter)[j])
                    function->merge(data, vec[j], arena.get());
        }
        else
        {
            for (size_t j = 0; j < size; ++j)
                function->merge(data, vec[j], arena.get());
        }
    }
}

}
