#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/Arena.h>
#include <Interpreters/Aggregator.h>
#include <Core/ColumnNumbers.h>


namespace DB
{

class ExpressionActions;


/** Takes blocks after grouping, with non-finalized aggregate functions.
  * Calculates all subsets of columns and aggreagetes over them.
  */
class CubeBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    using AggregateColumns = std::vector<ColumnRawPtrs>;
public:
    CubeBlockInputStream(
        const BlockInputStreamPtr & input_, const Aggregator::Params & params_);

    String getName() const override { return "Cube"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Aggregator aggregator;
    ColumnNumbers keys;
    UInt32 mask = 0;
    Block source_block;
    Block zero_block;
};

}
