#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>

namespace DB
{

/// Repartitions the data from M logical streams into N logical streams.
class ShuffleExchangeStep final : public LogicalExchangeStep
{
public:
    ShuffleExchangeStep(const Block & input_header_, Names key_names_, size_t result_bucket_count_)
        : LogicalExchangeStep(input_header_)
        , key_names(std::move(key_names_))
        , result_bucket_count(result_bucket_count_)   /// TODO: implement
    {
    }

    String getName() const override { return "ShuffleExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    size_t getResultBucketCount() const override
    {
        return result_bucket_count;
    }

    std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    const Names key_names;
    const size_t result_bucket_count;
};

}
