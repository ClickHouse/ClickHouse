#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

/// A pass-through step that buffers data into a QueryResultCacheWriter.
/// Inserted into the QueryPlan before SortingStep/LimitStep when caching
/// before LIMIT and ORDER BY is enabled.
class WriteToQueryResultCacheStep : public ITransformingStep
{
public:
    WriteToQueryResultCacheStep(
        const SharedHeader & input_header_,
        std::shared_ptr<QueryResultCacheWriter> cache_writer_);

    String getName() const override { return "WriteToQueryResultCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    std::shared_ptr<QueryResultCacheWriter> cache_writer;

    void updateOutputHeader() override { output_header = input_headers.front(); }
};

}
