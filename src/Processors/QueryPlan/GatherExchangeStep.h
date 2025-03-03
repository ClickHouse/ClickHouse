#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>

namespace DB
{

class GatherExchangeStep final : public LogicalExchangeStep
{
public:
    explicit GatherExchangeStep(const Block & input_header_)
        : LogicalExchangeStep(input_header_)
    {
    }

    String getName() const override { return "GatherExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    size_t getResultBucketCount() const override
    {
        return 1;
    }

    std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }
};

}
