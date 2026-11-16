#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Receive part of GatherExchangeStep
class GatherReceiveStep : public ISourceStep
{
public:
    GatherReceiveStep(SharedHeader header_, const String & exchange_id_, size_t num_buckets_,
                      std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , num_buckets(num_buckets_)
        , maintain_sort_description(std::move(maintain_sort_description_))
    {
    }

    String getName() const override { return "GatherReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    const String exchange_id;
    const size_t num_buckets;
    const std::optional<SortDescription> maintain_sort_description;
};


}
