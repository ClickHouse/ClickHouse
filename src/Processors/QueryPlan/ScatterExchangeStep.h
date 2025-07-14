#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>

namespace DB
{

/// Partitions the data from 1 logical streams into N logical streams.
class ScatterExchangeStep final : public LogicalExchangeStep
{
public:
    ScatterExchangeStep(SharedHeader input_header_, Names key_names_, size_t result_bucket_count_)
        : LogicalExchangeStep(input_header_)
        , key_names(std::move(key_names_))
        , result_bucket_count(result_bucket_count_)
    {
    }

    String getName() const override { return "ScatterExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    const Names & getKeys() const
    {
        return key_names;
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

    const Names key_names;
    const size_t result_bucket_count;
};

}
