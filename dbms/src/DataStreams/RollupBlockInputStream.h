#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/Arena.h>
#include <Interpreters/Aggregator.h>


namespace DB
{

class ExpressionActions;


/** Takes blocks after grouping, with non-finalized aggregate functions.
  * Calculates total values according to totals_mode.
  * If necessary, evaluates the expression from HAVING and filters rows. Returns the finalized and filtered blocks.
  */
class RollupBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    using AggregateColumns = std::vector<ColumnRawPtrs>;
public:
    /// expression may be nullptr
    RollupBlockInputStream(
        const BlockInputStreamPtr & input_, const Aggregator::Params & params_);

    String getName() const override { return "Rollup"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    size_t passed_keys = 0;
    size_t total_keys = 0;
    size_t not_aggregate_columns = 0;
    std::vector<std::vector<size_t>> group_borders;

    Aggregator::Params params;
    Aggregator aggregator;

    Block result_block;
    BlocksList blocks;

    /** Here are the values that did not pass max_rows_to_group_by.
      * They are added or not added to the current_totals, depending on the totals_mode.
      */

    /// Here, total values are accumulated. After the work is finished, they will be placed in IProfilingBlockInputStream::totals.
    MutableColumns current_totals;
    /// Arena for aggregate function states in totals.
    ArenaPtr arena;
    std::unique_ptr<IBlockInputStream> impl;

    /// If filter == nullptr - add all rows. Otherwise, only the rows that pass the filter (HAVING).
    void addToTotals(const Block & block);
    void createRollupScheme(const Block & block);
    void executeRollup(const Block & block);
};

}
