#include <Processors/QueryPlan/ReadFromQueryResultCacheStep.h>

namespace DB
{

namespace
{

Pipe makePipe(
    const std::shared_ptr<SourceFromChunks> & source,
    const std::shared_ptr<SourceFromChunks> & source_totals,
    const std::shared_ptr<SourceFromChunks> & source_extremes)
{
    auto pipe = Pipe();
    if (source)
        pipe.addSource(source);
    if (source_totals)
        pipe.addTotalsSource(source_totals);
    if (source_extremes)
        pipe.addExtremesSource(source_extremes);
    return pipe;
}

}

ReadFromQueryResultCacheStep::ReadFromQueryResultCacheStep(
    std::shared_ptr<SourceFromChunks> source_,
    std::shared_ptr<SourceFromChunks> source_totals_,
    std::shared_ptr<SourceFromChunks> source_extremes_)
    : ReadFromPreparedSource(makePipe(source_, source_totals_, source_extremes_))
    , source(std::move(source_))
    , source_totals(std::move(source_totals_))
    , source_extremes(std::move(source_extremes_))
{
}

QueryPlanStepPtr ReadFromQueryResultCacheStep::clone() const
{
    /// A cache hit replaces the whole subquery plan with this step, so without `clone` an `IN`
    /// subquery whose result comes from the query result cache would force
    /// `FutureSetFromSubquery::buildOrderedSetInplace` into its destructive fallback (consuming the
    /// canonical `source` plan), and a silent in-place build failure would leave the set permanently
    /// unbuilt. Cloning is cheap and side-effect free here: the chunks are already materialized and
    /// `Chunk::clone` shares their immutable columns, and replaying them does not touch the cache.
    auto clone_source = [](const std::shared_ptr<SourceFromChunks> & source_to_clone) -> std::shared_ptr<SourceFromChunks>
    {
        if (!source_to_clone)
            return nullptr;
        return source_to_clone->clone();
    };

    return std::make_unique<ReadFromQueryResultCacheStep>(
        clone_source(source), clone_source(source_totals), clone_source(source_extremes));
}

}
