#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>

namespace DB
{

/// Copies the data from 1 logical streams into N logical streams.
class BroadcastExchangeStep final : public LogicalExchangeStep
{
public:
BroadcastExchangeStep(SharedHeader input_header_, size_t result_bucket_count_)
        : LogicalExchangeStep(input_header_)
        , result_bucket_count(result_bucket_count_)
    {
    }

    String getName() const override { return "BroadcastExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    size_t getResultBucketCount() const override
    {
        return result_bucket_count;
    }

    std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    const size_t result_bucket_count;
};

}
