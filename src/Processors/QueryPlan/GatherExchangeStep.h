#pragma once

#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Core/SortDescription.h>

#include <optional>

namespace DB
{

/// Collects multiple logical streams of data with the same schema into one stream.
class GatherExchangeStep final : public LogicalExchangeStep
{
public:
    explicit GatherExchangeStep(SharedHeader input_header_, size_t source_bucket_count_, std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : LogicalExchangeStep(input_header_, std::move(maintain_sort_description_))
        , source_bucket_count(source_bucket_count_)
    {
    }

    String getName() const override { return "GatherExchange"; }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<GatherExchangeStep>(input_headers.front(), source_bucket_count, maintain_sort_description);
    }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    size_t getSourceBucketCount() const override
    {
        return source_bucket_count;
    }

    size_t getResultBucketCount() const override
    {
        return 1;
    }

    std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    const size_t source_bucket_count;
};

}
