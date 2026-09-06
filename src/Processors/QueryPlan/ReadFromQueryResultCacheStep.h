#pragma once

#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

class ReadFromQueryResultCacheStep : public ReadFromPreparedSource
{
public:
    ReadFromQueryResultCacheStep(
        std::shared_ptr<SourceFromChunks> source_,
        std::shared_ptr<SourceFromChunks> source_totals_,
        std::shared_ptr<SourceFromChunks> source_extremes_);

    String getName() const override { return "ReadFromQueryResultCacheStep"; }

    QueryPlanStepPtr clone() const override;

private:
    /// The very sources the `Pipe` of `ReadFromPreparedSource` is built from, kept so that `clone`
    /// can produce independent copies. The parent `ReadFromPreparedSource` stays non-clonable: a
    /// general prepared `Pipe` wraps single-use processors, whereas the query result cache always
    /// reads from replayable `SourceFromChunks`.
    std::shared_ptr<SourceFromChunks> source;
    std::shared_ptr<SourceFromChunks> source_totals;
    std::shared_ptr<SourceFromChunks> source_extremes;
};

}
