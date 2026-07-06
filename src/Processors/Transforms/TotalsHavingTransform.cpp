#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>

#include <Common/typeid_cast.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

static void finalizeBlock(Block & block, const ColumnsMask & aggregates_mask)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (!aggregates_mask[i])
            continue;

        ColumnWithTypeAndName & current = block.getByPosition(i);
        const DataTypeAggregateFunction & unfinalized_type = typeid_cast<const DataTypeAggregateFunction &>(*current.type);

        current.type = unfinalized_type.getReturnType();
        if (current.column)
        {
            auto mut_column = IColumn::mutate(std::move(current.column));
            current.column = ColumnAggregateFunction::convertToValues(std::move(mut_column));
        }
    }
}

Block TotalsHavingTransform::transformHeader(
    Block block,
    const ActionsDAG * expression,
    const std::string & filter_column_name,
    bool remove_filter,
    bool final,
    const ColumnsMask & aggregates_mask)
{
    if (final)
        finalizeBlock(block, aggregates_mask);

    if (expression)
    {
        block = expression->updateHeader(block);
        if (remove_filter)
            block.erase(filter_column_name);
    }

    return block;
}

TotalsHavingTransform::TotalsHavingTransform(
    const Block & header,
    const ColumnsMask & aggregates_mask_,
    bool overflow_row_,
    const ExpressionActionsPtr & expression_,
    const std::string & filter_column_,
    bool remove_filter_,
    TotalsMode totals_mode_,
    double auto_include_threshold_,
    bool final_)
    : ISimpleTransform(header, transformHeader(header, expression_  ? &expression_->getActionsDAG() : nullptr, filter_column_, remove_filter_, final_, aggregates_mask_), true)
    , aggregates_mask(aggregates_mask_)
    , overflow_row(overflow_row_)
    , expression(expression_)
    , filter_column_name(filter_column_)
    , remove_filter(remove_filter_)
    , totals_mode(totals_mode_)
    , auto_include_threshold(auto_include_threshold_)
    , final(final_)
{
    finalized_header = getInputPort().getHeader();
    finalizeBlock(finalized_header, aggregates_mask);

    /// Port for Totals.
    if (expression)
    {
        auto totals_header = finalized_header;
        size_t num_rows = totals_header.rows();
        expression->execute(totals_header, num_rows);
        filter_column_pos = totals_header.getPositionByName(filter_column_name);
        if (remove_filter)
            totals_header.erase(filter_column_name);
        outputs.emplace_back(totals_header, this);
    }
    else
    {
        if (!filter_column_name.empty())
            filter_column_pos = finalized_header.getPositionByName(filter_column_name);
        outputs.emplace_back(finalized_header, this);
    }

    /// Initialize current totals with initial state.
    current_totals.reserve(header.columns());
    for (const auto & elem : header)
    {
        MutableColumnPtr new_column = elem.type->createColumn();
        elem.type->insertDefaultInto(*new_column);
        current_totals.emplace_back(std::move(new_column));
    }
}

IProcessor::Status TotalsHavingTransform::prepare()
{
    if (!finished_transform)
    {
        auto status = ISimpleTransform::prepare();

        if (status != Status::Finished)
            return status;

        finished_transform = true;
    }

    auto & totals_output = getTotalsPort();

    /// Check can output.
    if (totals_output.isFinished())
        return Status::Finished;

    if (!totals_output.canPush())
        return Status::PortFull;

    if (!total_prepared)
        return Status::Ready;

    totals_output.push(std::move(totals));
    totals_output.finish();
    return Status::Finished;
}

void TotalsHavingTransform::work()
{
    if (finished_transform)
        prepareTotals();
    else
        ISimpleTransform::work();
}

void TotalsHavingTransform::transform(Chunk & chunk)
{
    /// Block with values not included in `max_rows_to_group_by`. We'll postpone it.
    if (overflow_row)
    {
        const auto & agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
        if (!agg_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo in TotalsHavingTransform.");

        if (agg_info->is_overflows)
        {
            overflow_aggregates = std::move(chunk);
            return;
        }
    }

    if (!chunk)
        return;

    auto finalized = chunk.clone();
    if (final)
        finalizeChunk(finalized, aggregates_mask);

    total_keys += finalized.getNumRows();

    if (filter_column_name.empty())
    {
        addToTotals(chunk, nullptr);
        chunk = std::move(finalized);
    }
    else
    {
        /// Compute the expression in HAVING.
        const auto & cur_header = final ? finalized_header : getInputPort().getHeader();
        size_t num_rows = finalized.getNumRows();
        auto finalized_block = cur_header.cloneWithColumns(finalized.detachColumns());

        for (const auto & action : expression->getActions())
        {
            if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Having clause cannot contain arrayJoin");
        }

        expression->execute(finalized_block, num_rows);
        ColumnPtr filter_column_ptr = finalized_block.getByPosition(filter_column_pos).column;
        if (remove_filter)
            finalized_block.erase(filter_column_name);
        auto columns = finalized_block.getColumns();

        ConstantFilterDescription const_filter_description(*filter_column_ptr);

        if (const_filter_description.always_true)
        {
            addToTotals(chunk, nullptr);
            chunk.setColumns(std::move(columns), num_rows);
            return;
        }

        if (const_filter_description.always_false)
        {
            if (totals_mode == TotalsMode::BEFORE_HAVING)
                addToTotals(chunk, nullptr);

            chunk.clear();
            return;
        }

        FilterDescription filter_description(*filter_column_ptr);

        /// Add values to `totals` (if it was not already done).
        if (totals_mode == TotalsMode::BEFORE_HAVING)
            addToTotals(chunk, nullptr);
        else
            addToTotals(chunk, filter_description.data);

        /// Filter the block by expression in HAVING.
        for (auto & column : columns)
        {
            column = column->filter(*filter_description.data, -1);
            if (column->empty())
            {
                chunk.clear();
                return;
            }
        }

        num_rows = columns.empty() ? countBytesInFilter(*filter_description.data) : columns.front()->size();
        chunk.setColumns(std::move(columns), num_rows);
    }

    passed_keys += chunk.getNumRows();
}

void TotalsHavingTransform::addToTotals(const Chunk & chunk, const IColumn::Filter * filter)
{
    auto num_columns = chunk.getNumColumns();
    for (size_t col = 0; col < num_columns; ++col)
    {
        const auto & current = chunk.getColumns()[col];

        if (const auto * column = typeid_cast<const ColumnAggregateFunction *>(current.get()))
        {
            auto & totals_column = typeid_cast<ColumnAggregateFunction &>(*current_totals[col]);
            assert(totals_column.size() == 1);

            /// Accumulate all aggregate states from a column of a source chunk into
            /// the corresponding totals column.
            const ColumnAggregateFunction::Container & vec = column->getData();
            size_t size = vec.size();

            if (filter && filter->size() != size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter has size which differs from column size");

            if (filter)
            {
                for (size_t row = 0; row < size; ++row)
                    if ((*filter)[row])
                        totals_column.insertMergeFrom(vec[row]);
            }
            else
            {
                for (size_t row = 0; row < size; ++row)
                    totals_column.insertMergeFrom(vec[row]);
            }
        }
    }
}

void TotalsHavingTransform::prepareTotals()
{
    /// If totals_mode == AFTER_HAVING_AUTO, you need to decide whether to add aggregates to TOTALS for strings,
    /// not passed max_rows_to_group_by.
    if (overflow_aggregates)
    {
        if (totals_mode == TotalsMode::BEFORE_HAVING
            || totals_mode == TotalsMode::AFTER_HAVING_INCLUSIVE
            || (totals_mode == TotalsMode::AFTER_HAVING_AUTO
                && static_cast<double>(passed_keys) / total_keys >= auto_include_threshold))
            addToTotals(overflow_aggregates, nullptr);
    }

    totals = Chunk(std::move(current_totals), 1);
    finalizeChunk(totals, aggregates_mask);

    if (expression)
    {
        size_t num_rows = totals.getNumRows();
        auto block = finalized_header.cloneWithColumns(totals.detachColumns());
        expression->execute(block, num_rows);
        if (remove_filter)
            block.erase(filter_column_name);
        /// Note: after expression totals may have several rows if `arrayJoin` was used in expression.
        totals = Chunk(block.getColumns(), num_rows);
    }

    total_prepared = true;
}

}
