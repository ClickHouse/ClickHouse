#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Core/Names.h>

namespace DB
{

/// Repartitions the data from M logical streams into N logical streams.
class ShuffleExchangeStep final : public LogicalExchangeStep
{
public:
    ShuffleExchangeStep(SharedHeader input_header_, Names key_names_, size_t source_bucket_count_, size_t result_bucket_count_)
        : LogicalExchangeStep(input_header_)
        , key_names(std::move(key_names_))
        , source_bucket_count(source_bucket_count_)
        , result_bucket_count(result_bucket_count_)
    {
    }

    String getName() const override { return "ShuffleExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    const Names & getKeys() const
    {
        return key_names;
    }

    size_t getSourceBucketCount() const override
    {
        return source_bucket_count;
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
    const size_t source_bucket_count;
    const size_t result_bucket_count;
};

}
