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

    Aggregator::Params params;
};

}
